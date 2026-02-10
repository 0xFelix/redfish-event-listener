package redfish

import (
	"encoding/json"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	redfishcommon "github.com/stmcginnis/gofish/common"
	"github.com/stmcginnis/gofish/redfish"

	"github.com/0xfelix/redfish-event-listener/pkg/common"
	"github.com/0xfelix/redfish-event-listener/pkg/redfish/wrapper"
)

var _ = Describe("CreateSubscriptionFromService", func() {
	const (
		destination = "https://example.org/destination"
		token       = "0123456789"
	)

	var eventService *fakeEventService

	BeforeEach(func() {
		eventService = &fakeEventService{
			GetEventSubscriptionsFunc: func() ([]*redfish.EventDestination, error) {
				return nil, nil
			},
		}
	})

	It("should create a new subscription", func() {
		const testURI = "test-uri"
		var wasCalled bool
		eventService.CreateEventSubscriptionFunc = func(dest, context string) (string, error) {
			Expect(dest).To(Equal(destination))
			Expect(context).To(Equal(common.EventContextPrefix + token))
			wasCalled = true
			return testURI, nil
		}

		uri, err := CreateSubscriptionFromService(destination, eventService, token)
		Expect(err).ToNot(HaveOccurred())
		Expect(wasCalled).To(BeTrue())
		Expect(uri).To(Equal(testURI))
	})

	It("should not create a new subscription if it already exists with the same token", func() {
		const existingURI = "existing-uri"
		eventService.GetEventSubscriptionsFunc = func() ([]*redfish.EventDestination, error) {
			return []*redfish.EventDestination{{
				Entity:      redfishcommon.Entity{ODataID: existingURI},
				Destination: destination,
				Context:     common.EventContextPrefix + token,
			}, {
				Entity:      redfishcommon.Entity{ODataID: "uri-2"},
				Destination: "different-destination",
				Context:     "different-context",
			}}, nil
		}

		eventService.CreateEventSubscriptionFunc = func(_, _ string) (string, error) {
			Fail("CreateEventSubscription should not be called")
			return "", nil
		}

		eventService.DeleteEventSubscriptionFunc = func(_ string) error {
			Fail("DeleteEventSubscription should not be called")
			return nil
		}

		uri, err := CreateSubscriptionFromService(destination, eventService, token)
		Expect(err).ToNot(HaveOccurred())
		Expect(uri).To(Equal(existingURI))
	})

	It("should remove previous subscription, before creating new one", func() {
		const uri1 = "uri-1"
		eventService.GetEventSubscriptionsFunc = func() ([]*redfish.EventDestination, error) {
			return []*redfish.EventDestination{{
				Entity:      redfishcommon.Entity{ODataID: uri1},
				Destination: destination,
				Context:     common.EventContextPrefix + "some-token",
			}, {
				Entity:      redfishcommon.Entity{ODataID: "uri-2"},
				Destination: "different-destination",
				Context:     "different-context",
			}}, nil
		}

		var deleteCalled bool
		eventService.DeleteEventSubscriptionFunc = func(uri string) error {
			Expect(uri).To(Equal(uri1))
			deleteCalled = true
			return nil
		}

		eventService.CreateEventSubscriptionFunc = func(_, _ string) (string, error) {
			return "test-uri", nil
		}

		_, err := CreateSubscriptionFromService(destination, eventService, token)
		Expect(err).ToNot(HaveOccurred())
		Expect(deleteCalled).To(BeTrue())
	})

	It("should use predefined subscription", func() {
		const uri1 = "uri-1"

		eventService.GetEventSubscriptionsFunc = func() ([]*redfish.EventDestination, error) {
			return []*redfish.EventDestination{{
				Entity:      redfishcommon.Entity{ODataID: uri1},
				Destination: "0.0.0.0",
			}, {
				Entity:      redfishcommon.Entity{ODataID: "uri-2"},
				Destination: "0.0.0.0",
			}}, nil
		}

		// Creating new subscription is not allowed for some manufacturers
		eventService.CreateEventSubscriptionFunc = func(_, _ string) (string, error) {
			return "", &redfishcommon.Error{
				HTTPReturnedStatusCode: http.StatusMethodNotAllowed,
			}
		}

		var patchCalled bool
		eventService.PatchEventSubscriptionFunc = func(uri string, patch *wrapper.EventDestinationPatch) error {
			Expect(uri).To(Equal(uri1))
			Expect(patch.Context).To(Equal(common.EventContextPrefix + token))
			Expect(patch.Destination).To(Equal(destination))
			Expect(patch.EventTypes).To(ContainElement(redfish.AlertEventType))
			Expect(patch.Protocol).To(Equal(redfish.RedfishEventDestinationProtocol))

			oem := map[string]any{}
			Expect(json.Unmarshal(patch.OEM, &oem)).To(Succeed())
			Expect(oem).To(HaveKey("Supermicro"))
			Expect(oem["Supermicro"]).To(HaveKeyWithValue("EnableSubscription", true))
			patchCalled = true
			return nil
		}

		uri, err := CreateSubscriptionFromService(destination, eventService, token)

		Expect(err).ToNot(HaveOccurred())
		Expect(uri).To(Equal(uri1))
		Expect(patchCalled).To(BeTrue())
	})
})

type fakeEventService struct {
	GetEventSubscriptionsFunc   func() ([]*redfish.EventDestination, error)
	CreateEventSubscriptionFunc func(string, string) (string, error)
	PatchEventSubscriptionFunc  func(uri string, patch *wrapper.EventDestinationPatch) error
	DeleteEventSubscriptionFunc func(uri string) error
}

var _ wrapper.EventServiceWrapper = &fakeEventService{}

func (f *fakeEventService) GetEventSubscriptions() ([]*redfish.EventDestination, error) {
	return f.GetEventSubscriptionsFunc()
}

func (f *fakeEventService) CreateEventSubscription(destination, context string) (string, error) {
	return f.CreateEventSubscriptionFunc(destination, context)
}

func (f *fakeEventService) PatchEventSubscription(uri string, patch *wrapper.EventDestinationPatch) error {
	return f.PatchEventSubscriptionFunc(uri, patch)
}

func (f *fakeEventService) DeleteEventSubscription(uri string) error {
	return f.DeleteEventSubscriptionFunc(uri)
}
