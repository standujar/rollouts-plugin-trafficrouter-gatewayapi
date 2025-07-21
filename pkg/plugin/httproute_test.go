package plugin

import (
	"context"
	"testing"

	"github.com/argoproj-labs/rollouts-plugin-trafficrouter-gatewayapi/internal/utils"
	"github.com/argoproj-labs/rollouts-plugin-trafficrouter-gatewayapi/pkg/mocks"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwFake "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/fake"
)

func TestIsHTTPRouteRef(t *testing.T) {
	httpRouteGroup := gatewayv1.Group("gateway.networking.k8s.io")
	httpRouteKind := gatewayv1.Kind("HTTPRoute")
	serviceKind := gatewayv1.Kind("Service")
	serviceGroup := gatewayv1.Group("")
	port := gatewayv1.PortNumber(80)

	tests := []struct {
		name     string
		ref      gatewayv1.HTTPBackendRef
		expected bool
	}{
		{
			name: "HTTPRoute reference",
			ref: gatewayv1.HTTPBackendRef{
				BackendRef: gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Group: &httpRouteGroup,
						Kind:  &httpRouteKind,
						Name:  "test-route",
					},
				},
			},
			expected: true,
		},
		{
			name: "Service reference",
			ref: gatewayv1.HTTPBackendRef{
				BackendRef: gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Group: &serviceGroup,
						Kind:  &serviceKind,
						Name:  "test-service",
						Port:  &port,
					},
				},
			},
			expected: false,
		},
		{
			name: "nil Group and Kind",
			ref: gatewayv1.HTTPBackendRef{
				BackendRef: gatewayv1.BackendRef{
					BackendObjectReference: gatewayv1.BackendObjectReference{
						Name: "test-service",
						Port: &port,
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isHTTPRouteRef(tt.ref)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindServiceInNestedHTTPRoutes(t *testing.T) {
	ctx := context.Background()
	
	// Create fake client with nested routes
	objects := []runtime.Object{
		&mocks.NestedHTTPRouteMainObj,
		&mocks.NestedHTTPRouteChildObj,
	}
	
	fakeClient := gwFake.NewSimpleClientset(objects...)
	httpRouteClient := fakeClient.GatewayV1().HTTPRoutes(mocks.RolloutNamespace)
	
	plugin := &RpcPlugin{
		LogCtx: utils.SetupLog(),
		IsTest: true,
	}
	
	tests := []struct {
		name         string
		serviceName  string
		startRoute   *gatewayv1.HTTPRoute
		expectPaths  int
		expectError  bool
	}{
		{
			name:        "Find stable service in nested route",
			serviceName: mocks.StableServiceName,
			startRoute:  &mocks.NestedHTTPRouteMainObj,
			expectPaths: 1,
			expectError: false,
		},
		{
			name:        "Find canary service in nested route",
			serviceName: mocks.CanaryServiceName,
			startRoute:  &mocks.NestedHTTPRouteMainObj,
			expectPaths: 1,
			expectError: false,
		},
		{
			name:        "Service not found",
			serviceName: "non-existent-service",
			startRoute:  &mocks.NestedHTTPRouteMainObj,
			expectPaths: 0,
			expectError: false,
		},
		{
			name:        "Direct service reference",
			serviceName: mocks.StableServiceName,
			startRoute:  &mocks.NestedHTTPRouteChildObj,
			expectPaths: 1,
			expectError: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			visited := make(map[string]bool)
			paths, err := plugin.findServiceInNestedHTTPRoutes(ctx, httpRouteClient, tt.startRoute, tt.serviceName, visited, nil)
			
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, paths, tt.expectPaths)
				
				// Verify path structure for successful finds
				if tt.expectPaths > 0 {
					for _, path := range paths {
						assert.Greater(t, len(path), 0)
						// Last element should contain the service
						lastRef := path[len(path)-1]
						backendRef := lastRef.Route.Spec.Rules[lastRef.RuleIndex].BackendRefs[lastRef.RefIndex]
						assert.Equal(t, tt.serviceName, string(backendRef.Name))
					}
				}
			}
		})
	}
}

func TestSetHTTPRouteWeightWithNesting(t *testing.T) {
	ctx := context.Background()
	var desiredWeight int32 = 30
	
	// Create fake client with nested routes
	objects := []runtime.Object{
		&mocks.NestedHTTPRouteMainObj,
		&mocks.NestedHTTPRouteChildObj,
	}
	
	fakeClient := gwFake.NewSimpleClientset(objects...)
	
	rollout := &v1alpha1.Rollout{
		Spec: v1alpha1.RolloutSpec{
			Strategy: v1alpha1.RolloutStrategy{
				Canary: &v1alpha1.CanaryStrategy{
					StableService: mocks.StableServiceName,
					CanaryService: mocks.CanaryServiceName,
				},
			},
		},
	}
	
	gatewayAPIConfig := &GatewayAPITrafficRouting{
		HTTPRoute: "nested-http-route-main",
		Namespace: mocks.RolloutNamespace,
	}
	
	plugin := &RpcPlugin{
		LogCtx:              utils.SetupLog(),
		IsTest:              true,
		HTTPRouteClient:     fakeClient.GatewayV1().HTTPRoutes(mocks.RolloutNamespace),
	}
	
	// Execute weight update
	err := plugin.setHTTPRouteWeight(rollout, desiredWeight, []v1alpha1.WeightDestination{}, gatewayAPIConfig)
	
	assert.Empty(t, err.ErrorString)
	
	// Verify that the child route was updated with correct weights
	childRoute, _ := fakeClient.GatewayV1().HTTPRoutes(mocks.RolloutNamespace).Get(ctx, "nested-http-route-child", metav1.GetOptions{})
	
	// Check stable service weight
	stableWeight := childRoute.Spec.Rules[0].BackendRefs[0].Weight
	assert.NotNil(t, stableWeight)
	assert.Equal(t, int32(70), *stableWeight) // 100 - 30
	
	// Check canary service weight
	canaryWeight := childRoute.Spec.Rules[0].BackendRefs[1].Weight
	assert.NotNil(t, canaryWeight)
	assert.Equal(t, desiredWeight, *canaryWeight)
}

func TestUpdateHTTPRouteWeights(t *testing.T) {
	ctx := context.Background()
	var desiredWeight int32 = 40
	
	// Create routes for testing
	route1 := mocks.HTTPRouteObj.DeepCopy()
	route1.Name = "route1"
	route2 := mocks.HTTPRouteObj.DeepCopy()
	route2.Name = "route2"
	
	objects := []runtime.Object{route1, route2}
	fakeClient := gwFake.NewSimpleClientset(objects...)
	httpRouteClient := fakeClient.GatewayV1().HTTPRoutes(mocks.RolloutNamespace)
	
	plugin := &RpcPlugin{
		LogCtx: utils.SetupLog(),
		IsTest: true,
	}
	
	// Create test paths
	canaryPaths := [][]HTTPRouteReference{
		{
			{Route: route1, RuleIndex: 0, RefIndex: 1}, // canary is second ref
		},
		{
			{Route: route2, RuleIndex: 0, RefIndex: 1}, // canary in route2 too
		},
	}
	
	stablePaths := [][]HTTPRouteReference{
		{
			{Route: route1, RuleIndex: 0, RefIndex: 0}, // stable is first ref
		},
		{
			{Route: route2, RuleIndex: 0, RefIndex: 0}, // stable in route2 too
		},
	}
	
	// Execute weight update
	err := plugin.updateHTTPRouteWeights(ctx, httpRouteClient, canaryPaths, stablePaths, desiredWeight)
	
	assert.NoError(t, err)
	
	// Verify both routes were updated
	for _, routeName := range []string{"route1", "route2"} {
		route, _ := httpRouteClient.Get(ctx, routeName, metav1.GetOptions{})
		
		// Check stable weight
		stableWeight := route.Spec.Rules[0].BackendRefs[0].Weight
		assert.NotNil(t, stableWeight)
		assert.Equal(t, int32(60), *stableWeight) // 100 - 40
		
		// Check canary weight
		canaryWeight := route.Spec.Rules[0].BackendRefs[1].Weight
		assert.NotNil(t, canaryWeight)
		assert.Equal(t, desiredWeight, *canaryWeight)
	}
}