package wrapper

import (
	"encoding/json"

	"github.com/stmcginnis/gofish/redfish"
)

type EventServiceWrapper interface {
	GetEventSubscriptions() ([]*redfish.EventDestination, error)
	CreateEventSubscription(destination, context string) (string, error)
	PatchEventSubscription(uri string, patch *EventDestinationPatch) error
	DeleteEventSubscription(uri string) error
}

func FromEventService(service *redfish.EventService) EventServiceWrapper {
	return &eventServiceWrapper{service}
}

// EventDestinationPatch is a struct that contains fields that we want to send in the patch request.
// It is simpler to send the patch directly, instead of using more complex code in the redfish library.
type EventDestinationPatch struct {
	Context     string                           `json:"Context,omitempty"`
	Destination string                           `json:"Destination,omitempty"`
	EventTypes  []redfish.EventType              `json:"EventTypes,omitempty"`
	OEM         json.RawMessage                  `json:"Oem,omitempty"`
	Protocol    redfish.EventDestinationProtocol `json:"Protocol,omitempty"`
}

type eventServiceWrapper struct {
	*redfish.EventService
}

var _ EventServiceWrapper = &eventServiceWrapper{}

func (e *eventServiceWrapper) CreateEventSubscription(destination, context string) (string, error) {
	// Do not use `deliveryRetryPolicy`, it does not work with HPE
	return e.CreateEventSubscriptionInstance(
		destination,
		nil,
		nil,
		nil,
		redfish.RedfishEventDestinationProtocol,
		context,
		"",
		nil,
	)
}

func (e *eventServiceWrapper) PatchEventSubscription(uri string, patch *EventDestinationPatch) error {
	return e.Patch(uri, patch)
}
