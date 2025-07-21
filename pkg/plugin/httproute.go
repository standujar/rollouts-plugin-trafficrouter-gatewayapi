package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/argoproj-labs/rollouts-plugin-trafficrouter-gatewayapi/internal/utils"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	pluginTypes "github.com/argoproj/argo-rollouts/utils/plugin/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayApiClientv1 "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1"
)

const (
	HTTPConfigMapKey = "httpManagedRoutes"
)

// Helper function to check if a backendRef is an HTTPRoute reference
func isHTTPRouteRef(backendRef gatewayv1.HTTPBackendRef) bool {
	// Check if Group and Kind point to an HTTPRoute
	return backendRef.BackendRef.BackendObjectReference.Group != nil &&
		*backendRef.BackendRef.BackendObjectReference.Group == "gateway.networking.k8s.io" &&
		backendRef.BackendRef.BackendObjectReference.Kind != nil &&
		*backendRef.BackendRef.BackendObjectReference.Kind == "HTTPRoute"
}

// HTTPRouteReference holds information about an HTTPRoute and its path to the target service
type HTTPRouteReference struct {
	Route     *gatewayv1.HTTPRoute
	RuleIndex int
	RefIndex  int
}

// findServiceInNestedHTTPRoutes recursively searches for a service in nested HTTPRoutes
func (r *RpcPlugin) findServiceInNestedHTTPRoutes(
	ctx context.Context,
	httpRouteClient gatewayApiClientv1.HTTPRouteInterface,
	currentRoute *gatewayv1.HTTPRoute,
	serviceName string,
	visited map[string]bool,
	path []HTTPRouteReference,
) ([][]HTTPRouteReference, error) {
	// Prevent infinite loops
	routeKey := fmt.Sprintf("%s/%s", currentRoute.Namespace, currentRoute.Name)
	if visited[routeKey] {
		return nil, nil
	}
	visited[routeKey] = true

	var allPaths [][]HTTPRouteReference

	// Check all rules in the current HTTPRoute
	for ruleIdx, rule := range currentRoute.Spec.Rules {
		for refIdx, backendRef := range rule.BackendRefs {
			// If this is a direct service reference
			if string(backendRef.Name) == serviceName && !isHTTPRouteRef(backendRef) {
				// Found the service, add current path
				currentPath := append([]HTTPRouteReference{}, path...)
				currentPath = append(currentPath, HTTPRouteReference{
					Route:     currentRoute,
					RuleIndex: ruleIdx,
					RefIndex:  refIdx,
				})
				allPaths = append(allPaths, currentPath)
			} else if isHTTPRouteRef(backendRef) {
				// This is an HTTPRoute reference, follow it
				nestedRouteName := string(backendRef.Name)
				nestedRoute, err := httpRouteClient.Get(ctx, nestedRouteName, metav1.GetOptions{})
				if err != nil {
					r.LogCtx.Info(fmt.Sprintf("Could not get nested HTTPRoute %s: %v", nestedRouteName, err))
					continue
				}

				// Add current route to path before recursing
				newPath := append([]HTTPRouteReference{}, path...)
				newPath = append(newPath, HTTPRouteReference{
					Route:     currentRoute,
					RuleIndex: ruleIdx,
					RefIndex:  refIdx,
				})

				// Recurse into nested HTTPRoute
				nestedPaths, err := r.findServiceInNestedHTTPRoutes(ctx, httpRouteClient, nestedRoute, serviceName, visited, newPath)
				if err != nil {
					return nil, err
				}
				allPaths = append(allPaths, nestedPaths...)
			}
		}
	}

	return allPaths, nil
}

// updateHTTPRouteWeights updates weights for all HTTPRoutes in the paths
func (r *RpcPlugin) updateHTTPRouteWeights(
	ctx context.Context,
	httpRouteClient gatewayApiClientv1.HTTPRouteInterface,
	canaryPaths [][]HTTPRouteReference,
	stablePaths [][]HTTPRouteReference,
	desiredWeight int32,
) error {
	updatedRoutes := make(map[string]*gatewayv1.HTTPRoute)

	// Update weights for canary paths
	for _, path := range canaryPaths {
		for _, ref := range path {
			routeKey := fmt.Sprintf("%s/%s", ref.Route.Namespace, ref.Route.Name)
			route, exists := updatedRoutes[routeKey]
			if !exists {
				// Clone the route
				route = ref.Route.DeepCopy()
				updatedRoutes[routeKey] = route
			}
			// Update weight
			route.Spec.Rules[ref.RuleIndex].BackendRefs[ref.RefIndex].Weight = &desiredWeight
		}
	}

	// Update weights for stable paths
	restWeight := 100 - desiredWeight
	for _, path := range stablePaths {
		for _, ref := range path {
			routeKey := fmt.Sprintf("%s/%s", ref.Route.Namespace, ref.Route.Name)
			route, exists := updatedRoutes[routeKey]
			if !exists {
				// Clone the route
				route = ref.Route.DeepCopy()
				updatedRoutes[routeKey] = route
			}
			// Update weight
			route.Spec.Rules[ref.RuleIndex].BackendRefs[ref.RefIndex].Weight = &restWeight
		}
	}

	// Apply all updates
	for _, route := range updatedRoutes {
		_, err := httpRouteClient.Update(ctx, route, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update HTTPRoute %s/%s: %w", route.Namespace, route.Name, err)
		}
		r.LogCtx.Info(fmt.Sprintf("Updated HTTPRoute %s/%s", route.Namespace, route.Name))
	}

	return nil
}

func (r *RpcPlugin) setHTTPRouteWeight(rollout *v1alpha1.Rollout, desiredWeight int32, additionalDestinations []v1alpha1.WeightDestination, gatewayAPIConfig *GatewayAPITrafficRouting) pluginTypes.RpcError {
	ctx := context.TODO()
	httpRouteClient := r.HTTPRouteClient
	if !r.IsTest {
		gatewayClientV1 := r.GatewayAPIClientset.GatewayV1()
		httpRouteClient = gatewayClientV1.HTTPRoutes(gatewayAPIConfig.Namespace)
	}
	httpRoute, err := httpRouteClient.Get(ctx, gatewayAPIConfig.HTTPRoute, metav1.GetOptions{})
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	canaryServiceName := rollout.Spec.Strategy.Canary.CanaryService
	stableServiceName := rollout.Spec.Strategy.Canary.StableService

	// Use recursive search to find services (will find direct refs first, then nested)
	visited := make(map[string]bool)

	// Find all paths to canary service
	canaryPaths, err := r.findServiceInNestedHTTPRoutes(ctx, httpRouteClient, httpRoute, canaryServiceName, visited, nil)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: fmt.Sprintf("failed to find canary service: %v", err),
		}
	}

	// Reset visited map for stable service search
	visited = make(map[string]bool)

	// Find all paths to stable service
	stablePaths, err := r.findServiceInNestedHTTPRoutes(ctx, httpRouteClient, httpRoute, stableServiceName, visited, nil)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: fmt.Sprintf("failed to find stable service: %v", err),
		}
	}

	if len(canaryPaths) == 0 {
		return pluginTypes.RpcError{
			ErrorString: fmt.Sprintf("canary service %s not found in HTTPRoute", canaryServiceName),
		}
	}

	if len(stablePaths) == 0 {
		return pluginTypes.RpcError{
			ErrorString: fmt.Sprintf("stable service %s not found in HTTPRoute", stableServiceName),
		}
	}

	// Update weights in all HTTPRoutes
	err = r.updateHTTPRouteWeights(ctx, httpRouteClient, canaryPaths, stablePaths, desiredWeight)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}

	// For test compatibility - update mock with the main route if it was updated
	if r.IsTest {
		for _, path := range append(canaryPaths, stablePaths...) {
			if len(path) > 0 && path[0].Route.Name == httpRoute.Name {
				// Re-fetch the updated route for the mock
				updatedRoute, err := httpRouteClient.Get(ctx, httpRoute.Name, metav1.GetOptions{})
				if err == nil {
					r.UpdatedHTTPRouteMock = updatedRoute
				}
				break
			}
		}
	}

	// Handle experiments if needed
	// Note: This might need adjustment for nested routes
	err = HandleExperiment(ctx, r.Clientset, r.GatewayAPIClientset, r.LogCtx, rollout, httpRoute, additionalDestinations)
	if err != nil {
		r.LogCtx.Error(err, "Failed to handle experiment services")
	}

	return pluginTypes.RpcError{}
}

func (r *RpcPlugin) setHTTPHeaderRoute(rollout *v1alpha1.Rollout, headerRouting *v1alpha1.SetHeaderRoute, gatewayAPIConfig *GatewayAPITrafficRouting) pluginTypes.RpcError {
	if headerRouting.Match == nil {
		managedRouteList := []v1alpha1.MangedRoutes{
			{
				Name: headerRouting.Name,
			},
		}
		return r.removeHTTPManagedRoutes(managedRouteList, gatewayAPIConfig)
	}
	ctx := context.TODO()
	httpRouteClient := r.HTTPRouteClient
	managedRouteMap := make(ManagedRouteMap)
	httpRouteName := gatewayAPIConfig.HTTPRoute
	clientset := r.TestClientset
	if !r.IsTest {
		gatewayClientv1 := r.GatewayAPIClientset.GatewayV1()
		httpRouteClient = gatewayClientv1.HTTPRoutes(gatewayAPIConfig.Namespace)
		clientset = r.Clientset.CoreV1().ConfigMaps(gatewayAPIConfig.Namespace)
	}
	configMap, err := utils.GetOrCreateConfigMap(gatewayAPIConfig.ConfigMap, utils.CreateConfigMapOptions{
		Clientset: clientset,
		Ctx:       ctx,
	})
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	err = utils.GetConfigMapData(configMap, HTTPConfigMapKey, &managedRouteMap)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	httpRoute, err := httpRouteClient.Get(ctx, httpRouteName, metav1.GetOptions{})
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	canaryServiceName := gatewayv1.ObjectName(rollout.Spec.Strategy.Canary.CanaryService)
	stableServiceName := rollout.Spec.Strategy.Canary.StableService
	canaryServiceKind := gatewayv1.Kind("Service")
	canaryServiceGroup := gatewayv1.Group("")
	httpHeaderRouteRuleList, rpcError := getHTTPHeaderRouteRuleList(headerRouting)
	if rpcError.HasError() {
		return rpcError
	}
	httpRouteRuleList := HTTPRouteRuleList(httpRoute.Spec.Rules)
	backendRefNameList := []string{string(canaryServiceName), stableServiceName}
	httpRouteRule, err := getRouteRule(httpRouteRuleList, backendRefNameList...)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	var canaryBackendRef *HTTPBackendRef
	for i := 0; i < len(httpRouteRule.BackendRefs); i++ {
		backendRef := httpRouteRule.BackendRefs[i]
		if canaryServiceName == backendRef.Name {
			canaryBackendRef = (*HTTPBackendRef)(&backendRef)
			break
		}
	}
	httpHeaderRouteRule := gatewayv1.HTTPRouteRule{
		Matches: []gatewayv1.HTTPRouteMatch{},
		BackendRefs: []gatewayv1.HTTPBackendRef{
			{
				BackendRef: gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Group: &canaryServiceGroup,
						Kind:  &canaryServiceKind,
						Name:  canaryServiceName,
						Port:  canaryBackendRef.Port,
					},
				},
			},
		},
	}
	for i := 0; i < len(httpRouteRule.Matches); i++ {
		httpHeaderRouteRule.Matches = append(httpHeaderRouteRule.Matches, gatewayv1.HTTPRouteMatch{
			Path:        httpRouteRule.Matches[i].Path,
			Headers:     httpHeaderRouteRuleList,
			QueryParams: httpRouteRule.Matches[i].QueryParams,
		})
	}
	httpRouteRuleList = append(httpRouteRuleList, httpHeaderRouteRule)
	oldHTTPRuleList := httpRoute.Spec.Rules
	httpRoute.Spec.Rules = httpRouteRuleList
	oldConfigMapData := make(ManagedRouteMap)
	err = utils.GetConfigMapData(configMap, HTTPConfigMapKey, &oldConfigMapData)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	taskList := []utils.Task{
		{
			Action: func() error {
				updatedHTTPRoute, err := httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
				if r.IsTest {
					r.UpdatedHTTPRouteMock = updatedHTTPRoute
				}
				if err != nil {
					return err
				}
				return nil
			},
			ReverseAction: func() error {
				httpRoute.Spec.Rules = oldHTTPRuleList
				updatedHTTPRoute, err := httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
				if r.IsTest {
					r.UpdatedHTTPRouteMock = updatedHTTPRoute
				}
				if err != nil {
					return err
				}
				return nil
			},
		},
		{
			Action: func() error {
				if managedRouteMap[headerRouting.Name] == nil {
					managedRouteMap[headerRouting.Name] = make(map[string]int)
				}
				managedRouteMap[headerRouting.Name][httpRouteName] = len(httpRouteRuleList) - 1
				err = utils.UpdateConfigMapData(configMap, managedRouteMap, utils.UpdateConfigMapOptions{
					Clientset:    clientset,
					ConfigMapKey: HTTPConfigMapKey,
					Ctx:          ctx,
				})
				if err != nil {
					return err
				}
				return nil
			},
			ReverseAction: func() error {
				err = utils.UpdateConfigMapData(configMap, oldConfigMapData, utils.UpdateConfigMapOptions{
					Clientset:    clientset,
					ConfigMapKey: HTTPConfigMapKey,
					Ctx:          ctx,
				})
				if err != nil {
					return err
				}
				return nil
			},
		},
	}
	err = utils.DoTransaction(r.LogCtx, taskList...)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	return pluginTypes.RpcError{}
}

func getHTTPHeaderRouteRuleList(headerRouting *v1alpha1.SetHeaderRoute) ([]gatewayv1.HTTPHeaderMatch, pluginTypes.RpcError) {
	httpHeaderRouteRuleList := []gatewayv1.HTTPHeaderMatch{}
	for _, headerRule := range headerRouting.Match {
		httpHeaderRouteRule := gatewayv1.HTTPHeaderMatch{
			Name: gatewayv1.HTTPHeaderName(headerRule.HeaderName),
		}
		switch {
		case headerRule.HeaderValue.Exact != "":
			headerMatchType := gatewayv1.HeaderMatchExact
			httpHeaderRouteRule.Type = &headerMatchType
			httpHeaderRouteRule.Value = headerRule.HeaderValue.Exact
		case headerRule.HeaderValue.Prefix != "":
			headerMatchType := gatewayv1.HeaderMatchRegularExpression
			httpHeaderRouteRule.Type = &headerMatchType
			httpHeaderRouteRule.Value = headerRule.HeaderValue.Prefix + ".*"
		case headerRule.HeaderValue.Regex != "":
			headerMatchType := gatewayv1.HeaderMatchRegularExpression
			httpHeaderRouteRule.Type = &headerMatchType
			httpHeaderRouteRule.Value = headerRule.HeaderValue.Regex
		default:
			return nil, pluginTypes.RpcError{
				ErrorString: InvalidHeaderMatchTypeError,
			}
		}
		httpHeaderRouteRuleList = append(httpHeaderRouteRuleList, httpHeaderRouteRule)
	}
	return httpHeaderRouteRuleList, pluginTypes.RpcError{}
}

func (r *RpcPlugin) removeHTTPManagedRoutes(managedRouteNameList []v1alpha1.MangedRoutes, gatewayAPIConfig *GatewayAPITrafficRouting) pluginTypes.RpcError {
	ctx := context.TODO()
	httpRouteClient := r.HTTPRouteClient
	clientset := r.TestClientset
	httpRouteName := gatewayAPIConfig.HTTPRoute
	managedRouteMap := make(ManagedRouteMap)
	if !r.IsTest {
		gatewayClientv1 := r.GatewayAPIClientset.GatewayV1()
		httpRouteClient = gatewayClientv1.HTTPRoutes(gatewayAPIConfig.Namespace)
		clientset = r.Clientset.CoreV1().ConfigMaps(gatewayAPIConfig.Namespace)
	}
	configMap, err := utils.GetOrCreateConfigMap(gatewayAPIConfig.ConfigMap, utils.CreateConfigMapOptions{
		Clientset: clientset,
		Ctx:       ctx,
	})
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	err = utils.GetConfigMapData(configMap, HTTPConfigMapKey, &managedRouteMap)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	httpRoute, err := httpRouteClient.Get(ctx, httpRouteName, metav1.GetOptions{})
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	httpRouteRuleList := HTTPRouteRuleList(httpRoute.Spec.Rules)
	isHTTPRouteRuleListChanged := false
	for _, managedRoute := range managedRouteNameList {
		managedRouteName := managedRoute.Name
		_, isOk := managedRouteMap[managedRouteName]
		if !isOk {
			r.LogCtx.Logger.Info(fmt.Sprintf("%s is not in httpHeaderManagedRouteMap", managedRouteName))
			continue
		}
		isHTTPRouteRuleListChanged = true
		httpRouteRuleList, err = removeManagedHTTPRouteEntry(managedRouteMap, httpRouteRuleList, managedRouteName, httpRouteName)
		if err != nil {
			return pluginTypes.RpcError{
				ErrorString: err.Error(),
			}
		}
	}
	if !isHTTPRouteRuleListChanged {
		return pluginTypes.RpcError{}
	}
	oldHTTPRuleList := httpRoute.Spec.Rules
	httpRoute.Spec.Rules = httpRouteRuleList
	oldConfigMapData := make(ManagedRouteMap)
	err = utils.GetConfigMapData(configMap, HTTPConfigMapKey, &oldConfigMapData)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	taskList := []utils.Task{
		{
			Action: func() error {
				updatedHTTPRoute, err := httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
				if r.IsTest {
					r.UpdatedHTTPRouteMock = updatedHTTPRoute
				}
				if err != nil {
					return err
				}
				return nil
			},
			ReverseAction: func() error {
				httpRoute.Spec.Rules = oldHTTPRuleList
				updatedHTTPRoute, err := httpRouteClient.Update(ctx, httpRoute, metav1.UpdateOptions{})
				if r.IsTest {
					r.UpdatedHTTPRouteMock = updatedHTTPRoute
				}
				if err != nil {
					return err
				}
				return nil
			},
		},
		{
			Action: func() error {
				err = utils.UpdateConfigMapData(configMap, managedRouteMap, utils.UpdateConfigMapOptions{
					Clientset:    clientset,
					ConfigMapKey: HTTPConfigMapKey,
					Ctx:          ctx,
				})
				if err != nil {
					return err
				}
				return nil
			},
			ReverseAction: func() error {
				err = utils.UpdateConfigMapData(configMap, oldConfigMapData, utils.UpdateConfigMapOptions{
					Clientset:    clientset,
					ConfigMapKey: HTTPConfigMapKey,
					Ctx:          ctx,
				})
				if err != nil {
					return err
				}
				return nil
			},
		},
	}
	err = utils.DoTransaction(r.LogCtx, taskList...)
	if err != nil {
		return pluginTypes.RpcError{
			ErrorString: err.Error(),
		}
	}
	return pluginTypes.RpcError{}
}

func removeManagedHTTPRouteEntry(managedRouteMap ManagedRouteMap, routeRuleList HTTPRouteRuleList, managedRouteName string, httpRouteName string) (HTTPRouteRuleList, error) {
	routeManagedRouteMap, isOk := managedRouteMap[managedRouteName]
	if !isOk {
		return nil, fmt.Errorf(ManagedRouteMapEntryDeleteError, managedRouteName, managedRouteName)
	}
	managedRouteIndex, isOk := routeManagedRouteMap[httpRouteName]
	if !isOk {
		managedRouteMapKey := managedRouteName + "." + httpRouteName
		return nil, fmt.Errorf(ManagedRouteMapEntryDeleteError, managedRouteMapKey, managedRouteMapKey)
	}
	delete(routeManagedRouteMap, httpRouteName)
	if len(managedRouteMap[managedRouteName]) == 0 {
		delete(managedRouteMap, managedRouteName)
	}
	for _, currentRouteManagedRouteMap := range managedRouteMap {
		value := currentRouteManagedRouteMap[httpRouteName]
		if value > managedRouteIndex {
			currentRouteManagedRouteMap[httpRouteName]--
		}
	}
	routeRuleList = slices.Delete(routeRuleList, managedRouteIndex, managedRouteIndex+1)
	return routeRuleList, nil
}

func (r *HTTPRouteRule) Iterator() (GatewayAPIRouteRuleIterator[*HTTPBackendRef], bool) {
	backendRefList := r.BackendRefs
	index := 0
	next := func() (*HTTPBackendRef, bool) {
		if len(backendRefList) == index {
			return nil, false
		}
		backendRef := (*HTTPBackendRef)(&backendRefList[index])
		index = index + 1
		return backendRef, len(backendRefList) > index
	}
	return next, len(backendRefList) > index
}

func (r HTTPRouteRuleList) Iterator() (GatewayAPIRouteRuleListIterator[*HTTPBackendRef, *HTTPRouteRule], bool) {
	routeRuleList := r
	index := 0
	next := func() (*HTTPRouteRule, bool) {
		if len(routeRuleList) == index {
			return nil, false
		}
		routeRule := (*HTTPRouteRule)(&routeRuleList[index])
		index++
		return routeRule, len(routeRuleList) > index
	}
	return next, len(routeRuleList) != index
}

func (r HTTPRouteRuleList) Error() error {
	return errors.New(BackendRefWasNotFoundInHTTPRouteError)
}

func (r *HTTPBackendRef) GetName() string {
	return string(r.Name)
}

func (r HTTPRoute) GetName() string {
	return r.Name
}
