package redfish

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/stmcginnis/gofish"
	redfishcommon "github.com/stmcginnis/gofish/common"
	"github.com/stmcginnis/gofish/redfish"

	"github.com/0xfelix/redfish-event-listener/pkg/common"
	"github.com/0xfelix/redfish-event-listener/pkg/redfish/wrapper"
	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

func CreateRedfishClient(nodeConfig *state.NodeConfig) (*gofish.APIClient, error) {
	config := gofish.ClientConfig{
		Endpoint:  nodeConfig.URL,
		Username:  nodeConfig.Username,
		Password:  nodeConfig.Password,
		Insecure:  nodeConfig.Insecure,
		BasicAuth: true,
	}

	return gofish.Connect(config)
}

func CreateSubscription(destinationURL string, nodeConfig *state.NodeConfig, token string) (string, error) {
	client, err := CreateRedfishClient(nodeConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	service, err := client.GetService().EventService()
	if err != nil {
		return "", fmt.Errorf("failed to get EventService: %w", err)
	}

	return CreateSubscriptionFromService(destinationURL, wrapper.FromEventService(service), token)
}

func CreateSubscriptionFromService(destinationURL string, service wrapper.EventServiceWrapper, token string) (string, error) {
	subs, err := service.GetEventSubscriptions()
	if err != nil {
		return "", fmt.Errorf("failed to get event subscriptions: %w", err)
	}

	for _, sub := range subs {
		foundToken, found := strings.CutPrefix(sub.Context, common.EventContextPrefix)
		if !found {
			continue
		}

		if foundToken == token {
			// The subscription already exists. We don't need to create it again.
			return sub.ODataID, nil
		}

		// Deleting subscription created previously with different token
		if deletionErr := service.DeleteEventSubscription(sub.ODataID); deletionErr != nil {
			return "", fmt.Errorf("failed to delete previous event subscription: %w", deletionErr)
		}
	}

	// We store the token in the context field, because not all redfish servers support the HttpHeaders field.
	subContext := common.EventContextPrefix + token
	uri, err := service.CreateEventSubscription(destinationURL, subContext)
	if err == nil {
		return uri, nil
	}

	var redfishErr *redfishcommon.Error
	if ok := errors.As(err, &redfishErr); !ok || redfishErr.HTTPReturnedStatusCode != http.StatusMethodNotAllowed {
		return "", fmt.Errorf("failed to create event subscription: %w", err)
	}

	// Fallback, try to get the event subscriptions, in vendors such as SuperMicro H/X12 they have a set of
	// predefined event subscriptions that are not created by the user
	return usePredefinedEventDestinationInstance(service, destinationURL, subContext)
}

func usePredefinedEventDestinationInstance(service wrapper.EventServiceWrapper, destinationURL, subContext string) (string, error) {
	subscriptions, err := service.GetEventSubscriptions()
	if err != nil {
		return "", fmt.Errorf("failed to get event subscriptions: %w", err)
	}

	// Let's find an unused event subscription and use it
	freeEventSubscription := getFreeEventSubscription(subscriptions)

	if freeEventSubscription == nil {
		return "", fmt.Errorf("failed to get a free event subscription")
	}

	patch := supermicroPatch(destinationURL, subContext)
	if err := service.PatchEventSubscription(freeEventSubscription.ODataID, patch); err != nil {
		return "", fmt.Errorf("failed to patch event subscription: %w", err)
	}

	return freeEventSubscription.ODataID, nil
}

func getFreeEventSubscription(subscriptions []*redfish.EventDestination) *redfish.EventDestination {
	for _, subscription := range subscriptions {
		if subscription.Destination == "0.0.0.0" || subscription.Destination == "" {
			return subscription
		}
	}
	return nil
}

func supermicroPatch(destinationURL, context string) *wrapper.EventDestinationPatch {
	return &wrapper.EventDestinationPatch{
		Context:     context,
		Destination: destinationURL,
		EventTypes:  []redfish.EventType{redfish.AlertEventType},
		OEM:         json.RawMessage(`{"Supermicro": {"EnableSubscription": true}}`),
		Protocol:    redfish.RedfishEventDestinationProtocol,
	}
}

func DeleteSubscription(subscriptionID string, nodeConfig *state.NodeConfig) error {
	client, err := CreateRedfishClient(nodeConfig)
	if err != nil {
		return fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	if err := redfish.DeleteEventDestination(client, subscriptionID); err != nil {
		return fmt.Errorf("failed to delete event subscription: %w", err)
	}

	return nil
}
