package far

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

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
	recheckInterval time.Duration,
	createSub CreateSubscriptionFunc,
	deleteSub DeleteSubscriptionFunc,
	mgr manager.Manager,
) error {
	// Lock is used so only one goroutine calls BMC and changes subscriptions state.
	// This prevents race where reconciler would change subscriptions, and periodic refresher would revert the changes.
	reconcilerLock := &sync.Mutex{}

	reconciler := NewReconciler(
		namespace,
		insecure,
		destinationURL,
		mgr.GetClient(),
		stateManager,
		createSub,
		deleteSub,
		reconcilerLock,
	)

	farTemplate := NewFarTemplateUnstructured()

	err := ctrl.NewControllerManagedBy(mgr).
		For(farTemplate, builder.WithPredicates(&specChangedPredicate{})).
		WithOptions(controller.Options{
			// The current implementation of Reconcile is not thread-safe.
			MaxConcurrentReconciles: 1,
		}).
		Complete(reconciler)
	if err != nil {
		return fmt.Errorf("failed adding FarTemplate controller to manager: %w", err)
	}

	refresher := NewSubscriptionRefresher(destinationURL, recheckInterval, stateManager, createSub, reconcilerLock)
	if err := mgr.Add(refresher); err != nil {
		return fmt.Errorf("failed adding subscription refresher to manager: %w", err)
	}

	return nil
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
