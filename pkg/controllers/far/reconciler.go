package far

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/0xfelix/redfish-event-listener/pkg/node"
	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

func NewReconciler(
	namespace string,
	insecure bool,
	destinationURL string,
	apiClient client.Client,
	infoState *node.NodeInfoState,
	createSub CreateSubscriptionFunc,
	deleteSub DeleteSubscriptionFunc,
) reconcile.Reconciler {
	return &farConfigReconciler{
		namespace:              namespace,
		insecure:               insecure,
		destinationURL:         destinationURL,
		client:                 apiClient,
		createSubscriptionFunc: createSub,
		deleteSubscriptionFunc: deleteSub,
		infoState:              infoState,
	}
}

type farConfigReconciler struct {
	namespace      string
	insecure       bool
	destinationURL string

	client                 client.Client
	createSubscriptionFunc CreateSubscriptionFunc
	deleteSubscriptionFunc DeleteSubscriptionFunc

	infoState *node.NodeInfoState
}

func (f *farConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Ignoring objects in other namespaces
	if req.Namespace != f.namespace {
		return ctrl.Result{}, nil
	}

	farObj := NewFarTemplateUnstructured()
	err := f.client.Get(ctx, req.NamespacedName, farObj)
	if k8serrors.IsNotFound(err) {
		// The FAR config was deleted. We need to remove subscriptions associated with it
		if deletionErr := f.deleteSubscriptionsForObj(req.Name); deletionErr != nil {
			return ctrl.Result{}, reconcile.TerminalError(deletionErr)
		}
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get FenceAgentsRemediationTemplate: %w", err)
	}

	isRelevant, err := isFarTemplateRelevant(farObj)
	if err != nil {
		return ctrl.Result{}, reconcile.TerminalError(err)
	}
	if !isRelevant {
		// Maybe the FAR template was relevant before, so we need to remove any existing subscriptions created from it.
		if deletionErr := f.deleteSubscriptionsForObj(farObj.GetName()); deletionErr != nil {
			return ctrl.Result{}, reconcile.TerminalError(deletionErr)
		}
		return ctrl.Result{}, nil
	}

	nodeConfigs, err := nodeConfigsFromFar(farObj, f.insecure)
	if err != nil {
		return ctrl.Result{}, reconcile.TerminalError(fmt.Errorf("failed to parse FenceAgentsRemediationTemplate: %w", err))
	}

	configsByName := map[string]state.NodeConfig{}
	for _, nodeConfig := range nodeConfigs {
		name := nodeConfig.NodeName
		configsByName[name] = nodeConfig
	}

	if err := f.updateSubscriptions(configsByName, farObj.GetName()); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update subscriptions: %w", err)
	}

	return ctrl.Result{}, nil
}

func isFarTemplateRelevant(obj *unstructured.Unstructured) (bool, error) {
	agent, found, err := unstructured.NestedString(obj.Object, "spec", "template", "spec", "agent")
	if err != nil {
		return false, fmt.Errorf("failed to get .spec.template.spec.agent: %w", err)
	}
	if !found {
		log.Printf("Skipped FenceAgentsRemediationTemplates \"%s/%s\", no agent is defined.", obj.GetNamespace(), obj.GetName())
		return false, nil
	}

	// Ignoring other agents
	if agent != "fence_ipmilan" {
		log.Printf("Skipped FenceAgentsRemediationTemplates \"%s/%s\", because its agent is %q", obj.GetNamespace(), obj.GetName(), agent)
		return false, nil
	}

	return true, nil
}

func (f *farConfigReconciler) updateSubscriptions(configsByName map[string]state.NodeConfig, farObjName string) error {
	f.infoState.Lock.Lock()
	defer f.infoState.Lock.Unlock()

	var resultErrors []error

	// Remove old subscriptions
	for nodeName, nodeInfo := range f.infoState.Subs {
		if nodeInfo.FarTemplateName != farObjName {
			continue
		}
		if _, ok := configsByName[nodeName]; ok {
			continue
		}
		resultErrors = append(resultErrors, f.deleteSubscriptionLocked(&nodeInfo))
	}

	// Create new subscriptions
	for nodeName, config := range configsByName {
		if _, exists := f.infoState.Subs[nodeName]; exists {
			continue
		}

		log.Printf("Monitoring node: %s", config.NodeName)

		// Use cryptographically random token.
		token := rand.Text()

		if err := f.createSubscriptionLocked(config, token, farObjName); err != nil {
			resultErrors = append(resultErrors, err)
		}
	}

	return errors.Join(resultErrors...)
}

func (f *farConfigReconciler) deleteSubscriptionsForObj(objName string) error {
	f.infoState.Lock.Lock()
	defer f.infoState.Lock.Unlock()

	var resultErrors []error
	for _, nodeInfo := range f.infoState.Subs {
		if nodeInfo.FarTemplateName == objName {
			resultErrors = append(resultErrors, f.deleteSubscriptionLocked(&nodeInfo))
		}
	}

	return errors.Join(resultErrors...)
}

func (f *farConfigReconciler) createSubscriptionLocked(config state.NodeConfig, token, farObjName string) error {
	// This function assumes that write lock f.infoState.Lock() is held

	subscriptionID, err := f.createSubscriptionFunc(f.destinationURL, &config, token)
	if err != nil {
		return fmt.Errorf("failed to create subscription for node %q: %w", config.NodeName, err)
	}

	f.infoState.Subs[config.NodeName] = state.Subscription{
		NodeConfig:      config,
		URI:             subscriptionID,
		FarTemplateName: farObjName,
		Token:           token,
	}
	f.infoState.TokenToName[token] = config.NodeName

	log.Printf("Created Redfish event subscription: %s", subscriptionID)
	return nil
}

func (f *farConfigReconciler) deleteSubscriptionLocked(sub *state.Subscription) error {
	// This function assumes that write lock f.infoState.Lock() is held
	var result error
	if sub.URI != "" {
		log.Printf("Deleting Redfish event subscription: %s", sub.URI)
		if err := f.deleteSubscriptionFunc(sub.URI, &sub.NodeConfig); err != nil {
			result = fmt.Errorf("failed to delete subscription for node %q: %w", sub.NodeConfig.NodeName, err)
		}
	}
	// The node is deleted from infoState, even if the subscription deletion above failed.
	// In that case, the BMC will still try to send events, but this pod will ignore them.
	delete(f.infoState.TokenToName, sub.Token)
	delete(f.infoState.Subs, sub.NodeConfig.NodeName)
	return result
}

func nodeConfigsFromFar(obj *unstructured.Unstructured, insecure bool) ([]state.NodeConfig, error) {
	nodeParameters, found, err := unstructured.NestedMap(obj.Object, "spec", "template", "spec", "nodeparameters")
	if !found {
		return nil, fmt.Errorf("failed to find .spec.template.spec.nodeparameters")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get .spec.template.spec.nodeparameters: %w", err)
	}

	ips, found, err := unstructured.NestedStringMap(nodeParameters, "--ip")
	if !found {
		return nil, fmt.Errorf("failed to find '--ip' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--ip' parameter: %w", err)
	}
	users, found, err := unstructured.NestedStringMap(nodeParameters, "--username")
	if !found {
		return nil, fmt.Errorf("failed to find '--username' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--username' parameter: %w", err)
	}
	passwords, found, err := unstructured.NestedStringMap(nodeParameters, "--password")
	if !found {
		return nil, fmt.Errorf("failed to find '--password' parameter")
	}
	if err != nil {
		return nil, fmt.Errorf("error getting '--password' parameter: %w", err)
	}

	var nodeConfigs []state.NodeConfig
	for nodeName, ip := range ips {
		user, ok := users[nodeName]
		if !ok {
			log.Printf("FAR config does not specify username for node %q, ignoring the node.", nodeName)
			continue
		}
		password, ok := passwords[nodeName]
		if !ok {
			log.Printf("FAR config does not specify password for node %q, ignoring the node.", nodeName)
			continue
		}
		nodeConfigs = append(nodeConfigs, state.NodeConfig{
			NodeName: nodeName,
			URL:      fmt.Sprintf("https://%s", ip),
			Username: user,
			Password: password,
			Insecure: insecure,
		})
	}

	return nodeConfigs, nil
}
