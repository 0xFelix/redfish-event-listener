package far

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/0xfelix/redfish-event-listener/pkg/node"
)

type (
	CreateSubscriptionFunc func(destinationURL string, nodeConfig *node.NodeConfig, token string) (string, error)
	DeleteSubscriptionFunc func(subscriptionURI string, nodeConfig *node.NodeConfig) error
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
	infoState *node.NodeInfoState,
	createSub CreateSubscriptionFunc,
	deleteSub DeleteSubscriptionFunc,
	mgr manager.Manager,
) error {
	reconciler := NewReconciler(
		namespace,
		insecure,
		destinationURL,
		mgr.GetClient(),
		infoState,
		createSub,
		deleteSub,
	)

	farTemplate := NewFarTemplateUnstructured()

	return ctrl.NewControllerManagedBy(mgr).
		For(farTemplate).
		Complete(reconciler)
}
