package far

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	redfishcommon "github.com/stmcginnis/gofish/common"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
	"github.com/0xfelix/redfish-event-listener/pkg/statemanager"
)

func NewReconciler(
	namespace string,
	insecure bool,
	destinationURL string,
	apiClient client.Client,
	stateManager statemanager.StateManager,
	createSub CreateSubscriptionFunc,
	deleteSub DeleteSubscriptionFunc,
	reconcilerLock *sync.Mutex,
) reconcile.Reconciler {
	return &farConfigReconciler{
		namespace:              namespace,
		insecure:               insecure,
		destinationURL:         destinationURL,
		client:                 apiClient,
		stateManager:           stateManager,
		createSubscriptionFunc: createSub,
		deleteSubscriptionFunc: deleteSub,
		reconcilerLock:         reconcilerLock,
	}
}

type farConfigReconciler struct {
	namespace      string
	insecure       bool
	destinationURL string

	client                 client.Client
	stateManager           statemanager.StateManager
	createSubscriptionFunc CreateSubscriptionFunc
	deleteSubscriptionFunc DeleteSubscriptionFunc

	reconcilerLock *sync.Mutex
}

func (f *farConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if req.Namespace != f.namespace {
		return ctrl.Result{}, nil
	}

	f.reconcilerLock.Lock()
	defer f.reconcilerLock.Unlock()

	currentSubs := f.stateManager.GetSubscriptions()
	newSubs, reconcileErr := f.reconcileTemplate(ctx, req, currentSubs)
	f.stateManager.SetSubscriptions(newSubs)

	return ctrl.Result{}, reconcileErr
}

func (f *farConfigReconciler) reconcileTemplate(
	ctx context.Context,
	req ctrl.Request,
	currentSubs []state.Subscription,
) ([]state.Subscription, error) {
	farObj := NewFarTemplateUnstructured()
	err := f.client.Get(ctx, req.NamespacedName, farObj)
	if k8serrors.IsNotFound(err) {
		// The FAR config was deleted. We need to remove subscriptions associated with it
		modifiedSubs, deletionErr := f.deleteSubscriptionsForObj(currentSubs, req.Name)
		if deletionErr != nil {
			return modifiedSubs, reconcile.TerminalError(deletionErr)
		}
		return modifiedSubs, nil
	}
	if err != nil {
		return currentSubs, fmt.Errorf("failed to get FenceAgentsRemediationTemplate: %w", err)
	}

	isRelevant, err := isFarTemplateRelevant(farObj)
	if err != nil {
		return currentSubs, reconcile.TerminalError(err)
	}
	if !isRelevant {
		// Maybe the FAR template was relevant before, so we need to remove any existing subscriptions created from it.
		modifiedSubs, deletionErr := f.deleteSubscriptionsForObj(currentSubs, farObj.GetName())
		if deletionErr != nil {
			return modifiedSubs, reconcile.TerminalError(deletionErr)
		}
		return modifiedSubs, nil
	}

	nodeConfigs, err := nodeConfigsFromFar(farObj, f.insecure)
	if err != nil {
		return currentSubs, reconcile.TerminalError(fmt.Errorf("failed to parse FenceAgentsRemediationTemplate: %w", err))
	}

	return f.updateSubscriptions(nodeConfigs, farObj.GetName(), currentSubs)
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

func (f *farConfigReconciler) updateSubscriptions(
	nodeConfigsByName map[string]state.NodeConfig,
	farObjName string,
	currentSubs []state.Subscription,
) ([]state.Subscription, error) {
	result := make([]state.Subscription, 0, len(currentSubs))
	var resultErrors []error

	keepSubs := map[string]state.Subscription{}
	for _, sub := range currentSubs {
		if sub.FarTemplateName != farObjName {
			result = append(result, sub)
			continue
		}

		if _, ok := nodeConfigsByName[sub.NodeConfig.NodeName]; !ok {
			resultErrors = append(resultErrors, f.deleteSubscription(&sub))
			continue
		}
		keepSubs[sub.NodeConfig.NodeName] = sub
	}

	// Add new subscriptions to "keepSubs"
	for _, config := range nodeConfigsByName {
		if _, exists := keepSubs[config.NodeName]; exists {
			continue
		}

		log.Printf("Monitoring node: %s", config.NodeName)

		keepSubs[config.NodeName] = state.Subscription{
			NodeConfig:      config,
			URI:             "",
			FarTemplateName: farObjName,
			// Use cryptographically random token.
			Token: rand.Text(),
		}
	}

	for _, sub := range keepSubs {
		// f.createSubscriptionFunc() does not add subscription to nodes where it already exists
		subURI, err := f.createSubscriptionFunc(f.destinationURL, &sub.NodeConfig, sub.Token)
		if err != nil {
			resultErrors = append(resultErrors, fmt.Errorf("failed to create subscription for node %q: %w", sub.NodeConfig.NodeName, err))
		} else {
			sub.URI = subURI
		}
		result = append(result, sub)
	}

	return result, errors.Join(resultErrors...)
}

func (f *farConfigReconciler) deleteSubscriptionsForObj(currentSubs []state.Subscription, objName string) ([]state.Subscription, error) {
	var result []state.Subscription
	var resultErrors []error
	for _, sub := range currentSubs {
		if sub.FarTemplateName != objName {
			result = append(result, sub)
			continue
		}

		if err := f.deleteSubscription(&sub); err != nil {
			resultErrors = append(resultErrors, err)
		}
	}

	return result, errors.Join(resultErrors...)
}

func (f *farConfigReconciler) deleteSubscription(sub *state.Subscription) error {
	if sub.URI != "" {
		log.Printf("Deleting Redfish event subscription: %s", sub.URI)
		if err := f.deleteSubscriptionFunc(sub.URI, &sub.NodeConfig); err != nil {
			var redfishErr *redfishcommon.Error
			if ok := errors.As(err, &redfishErr); ok && redfishErr.HTTPReturnedStatusCode == http.StatusNotFound {
				// Subscription may have been already deleted
				return nil
			}
			return fmt.Errorf("failed to delete subscription for node %q: %w", sub.NodeConfig.NodeName, err)
		}
	}
	return nil
}

func nodeConfigsFromFar(obj *unstructured.Unstructured, insecure bool) (map[string]state.NodeConfig, error) {
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

	nodeConfigs := map[string]state.NodeConfig{}
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
		if _, ok = nodeConfigs[nodeName]; ok {
			log.Printf("FAR config already contains node %q, ignoring the node.", nodeName)
			continue
		}

		nodeConfigs[nodeName] = state.NodeConfig{
			NodeName: nodeName,
			URL:      fmt.Sprintf("https://%s", ip),
			Username: user,
			Password: password,
			Insecure: insecure,
		}
	}

	return nodeConfigs, nil
}
