package redfish

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/stmcginnis/gofish"
	"github.com/stmcginnis/gofish/common"
	"github.com/stmcginnis/gofish/redfish"

	"github.com/0xfelix/redfish-event-listener/pkg/node"
)

var readWriteSubscriptionFields = []string{
	"Context",
	"DeliveryRetryPolicy",
	"VerifyCertificate",
	"Destination",
	"EventTypes",
	"Protocol",
	"Oem",
	"SubscriptionType",
}

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

func CreateSubscription(destinationURL string, nodeConfig *node.NodeConfig, eventContext string) (string, error) {
	client, err := CreateRedfishClient(nodeConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create Redfish client: %w", err)
	}
	defer client.Logout()

	service, err := client.GetService().EventService()
	if err != nil {
		return "", fmt.Errorf("failed to get EventService: %w", err)
	}

	return createEventDestinationInstance(client, service, destinationURL, eventContext)
}

func createEventDestinationInstance(client *gofish.APIClient, service *redfish.EventService,
	destinationURL, subContext string,
) (string, error) {
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

	var redfishErr *common.Error
	if ok := errors.As(err, &redfishErr); !ok || redfishErr.HTTPReturnedStatusCode != http.StatusMethodNotAllowed {
		return "", fmt.Errorf("failed to create event subscription: %w", err)
	}

	// Fallback, try to get the event subscriptions, in vendors such as SuperMicro H/X12 they have a set of
	// predefined event subscriptions that are not created by the user
	return usePredefinedEventDestinationInstance(service, destinationURL, subContext)
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

	modifiedEventSubscription := modifySupermicroEventSubscription(freeEventSubscription, destinationURL, subContext)

	if err := updateEventSubscription(freeEventSubscription, modifiedEventSubscription); err != nil {
		return "", fmt.Errorf("failed to update event subscription: %w", err)
	}

	return modifiedEventSubscription.ODataID, nil
}

func getFreeEventSubscription(subscriptions []*redfish.EventDestination) *redfish.EventDestination {
	for _, subscription := range subscriptions {
		if subscription.Destination == "0.0.0.0" || subscription.Destination == "" {
			return subscription
		}
	}
	return nil
}

func modifySupermicroEventSubscription(
	eventSubscription *redfish.EventDestination,
	destinationURL,
	subContext string,
) *redfish.EventDestination {
	// Create shallow copy of originalEventSubscription, only new values are
	// assigned to fields below (top-level fields only)
	newEventSubscription := *eventSubscription

	newEventSubscription.Destination = destinationURL
	newEventSubscription.Context = subContext
	newEventSubscription.EventTypes = []redfish.EventType{redfish.AlertEventType}
	newEventSubscription.Protocol = redfish.RedfishEventDestinationProtocol
	newEventSubscription.OEM = json.RawMessage(
		[]byte(`{"Supermicro": {"EnableSubscription": true}}`),
	)
	return &newEventSubscription
}

func updateEventSubscription(originalEventSubscription *redfish.EventDestination,
	updatedEventSubscription *redfish.EventDestination,
) error {
	originalElement := reflect.ValueOf(originalEventSubscription).Elem()
	currentElement := reflect.ValueOf(updatedEventSubscription).Elem()

	return updatedEventSubscription.Entity.Update(originalElement, currentElement, readWriteSubscriptionFields)
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
