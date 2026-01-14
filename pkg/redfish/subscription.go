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
	"github.com/0xfelix/redfish-event-listener/pkg/node"
)

func CreateRedfishClient(nodeConfig *node.NodeConfig) (*gofish.APIClient, error) {
	config := gofish.ClientConfig{
		Endpoint:  nodeConfig.URL,
		Username:  nodeConfig.Username,
		Password:  nodeConfig.Password,
		Insecure:  nodeConfig.Insecure,
		BasicAuth: true,
	}

	return gofish.Connect(config)
}

func CreateSubscription(destinationURL string, nodeConfig *node.NodeConfig, token string) (string, error) {
	client, err := CreateRedfishClient(nodeConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	service, err := client.GetService().EventService()
	if err != nil {
		return "", fmt.Errorf("failed to get EventService: %w", err)
	}

	if err = removePreviousEventDestinations(service); err != nil {
		return "", fmt.Errorf("failed to remove previous subscriptions: %w", err)
	}

	return createEventDestinationInstance(client, service, destinationURL, token)
}

func createEventDestinationInstance(client *gofish.APIClient, service *redfish.EventService,
	destinationURL, token string,
) (string, error) {
	// We store the token in the context field, because not all redfish servers support the HttpHeaders field.
	subContext := common.EventContextPrefix + token

	// Do not use `deliveryRetryPolicy`, it does not work with HPE
	uri, err := redfish.CreateEventDestinationInstance(
		client, service.Subscriptions, destinationURL,
		nil,
		nil,
		nil,
		redfish.RedfishEventDestinationProtocol,
		subContext,
		"",
		nil,
	)

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

func removePreviousEventDestinations(service *redfish.EventService) error {
	subs, err := service.GetEventSubscriptions()
	if err != nil {
		return fmt.Errorf("failed to get event subscriptions: %w", err)
	}

	for _, sub := range subs {
		if !strings.HasPrefix(sub.Context, common.EventContextPrefix) {
			continue
		}

		if err := service.DeleteEventSubscription(sub.ODataID); err != nil {
			return fmt.Errorf("failed to delete event subscription: %w", err)
		}
	}

	return nil
}

func usePredefinedEventDestinationInstance(service *redfish.EventService, destinationURL, subContext string) (string, error) {
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
	if err := service.Patch(freeEventSubscription.ODataID, patch); err != nil {
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

// eventDestinationPatch is a struct that contains fields that we want to send in the patch request.
// It is simpler to send the patch directly, instead of using more complex code in the redfish library.
type eventDestinationPatch struct {
	Context     string                           `json:"Context,omitempty"`
	Destination string                           `json:"Destination,omitempty"`
	EventTypes  []redfish.EventType              `json:"EventTypes,omitempty"`
	OEM         json.RawMessage                  `json:"Oem,omitempty"`
	Protocol    redfish.EventDestinationProtocol `json:"Protocol,omitempty"`
}

func supermicroPatch(destinationURL, context string) *eventDestinationPatch {
	return &eventDestinationPatch{
		Context:     context,
		Destination: destinationURL,
		EventTypes:  []redfish.EventType{redfish.AlertEventType},
		OEM:         json.RawMessage(`{"Supermicro": {"EnableSubscription": true}}`),
		Protocol:    redfish.RedfishEventDestinationProtocol,
	}
}

func DeleteSubscription(subscriptionID string, nodeConfig *node.NodeConfig) error {
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
