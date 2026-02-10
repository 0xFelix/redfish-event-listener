package far

import (
	"log"
	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
	"github.com/0xfelix/redfish-event-listener/pkg/statemanager"
)

type (
	CreateSubscriptionFunc func(destinationURL string, nodeConfig *state.NodeConfig, token string) (string, error)
	DeleteSubscriptionFunc func(subscriptionURI string, nodeConfig *state.NodeConfig) error
)

func FarTemplateGVK() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   "fence-agents-remediation.medik8s.io",
		Version: "v1alpha1",
		Kind:    "FenceAgentsRemediationTemplate",
	}
}

func NewFarTemplateUnstructured() *unstructured.Unstructured {
	farTemplate := &unstructured.Unstructured{}
	farTemplate.SetGroupVersionKind(FarTemplateGVK())
	return farTemplate
}

func AddToManager(
	namespace string,
	insecure bool,
	destinationURL string,
	stateManager statemanager.StateManager,
	createSub CreateSubscriptionFunc,
	deleteSub DeleteSubscriptionFunc,
	mgr manager.Manager,
) error {
	reconciler := NewReconciler(
		namespace,
		insecure,
		destinationURL,
		mgr.GetClient(),
		stateManager,
		createSub,
		deleteSub,
	)

	farTemplate := NewFarTemplateUnstructured()

	return ctrl.NewControllerManagedBy(mgr).
		For(farTemplate, builder.WithPredicates(&specChangedPredicate{})).
		WithOptions(controller.Options{
			// The current implementation of Reconcile is not thread-safe.
			MaxConcurrentReconciles: 1,
		}).
		Complete(reconciler)
}

type specChangedPredicate struct {
	predicate.Funcs
}

var _ predicate.Predicate = &specChangedPredicate{}

func (s *specChangedPredicate) Update(e event.UpdateEvent) bool {
	oldFarTemplate, ok := e.ObjectOld.(*unstructured.Unstructured)
	if !ok {
		log.Printf("Unexpected event type: %T", e.ObjectOld)
		return false
	}

	newFarTemplate, ok := e.ObjectNew.(*unstructured.Unstructured)
	if !ok {
		log.Printf("Unexpected event type: %T", e.ObjectNew)
		return false
	}

	oldSpec, oldSpecExists := oldFarTemplate.Object["spec"]
	newSpec, newSpecExists := newFarTemplate.Object["spec"]

	if !oldSpecExists && !newSpecExists {
		return false
	}
	if oldSpecExists != newSpecExists {
		return true
	}
	return !reflect.DeepEqual(oldSpec, newSpec)
}
