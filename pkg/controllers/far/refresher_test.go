package far_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/0xfelix/redfish-event-listener/pkg/controllers/far"
	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

type subRefresher interface {
	manager.Runnable
	RefreshSubscriptions()
}

var _ = Describe("Subscription Refresher", func() {
	const (
		shortInterval  = 10 * time.Millisecond
		destinationURL = "http://example.com/events"
		farName        = "test-far"
		node1Name      = "node1"
		node2Name      = "node2"
		token1         = "token1"
		token2         = "token2"
		sub1           = "sub-1"
		sub2           = "sub-2"
		newSub         = "sub-new"
	)

	var (
		stateMgr      *testStateManager
		createSubFunc far.CreateSubscriptionFunc
		refresher     subRefresher
	)

	BeforeEach(func() {
		stateMgr = &testStateManager{
			State: &state.State{
				Version:       state.VersionV1,
				Subscriptions: []state.Subscription{},
			},
		}
		createSubFunc = nil
		refresher = far.NewSubscriptionRefresher(
			destinationURL,
			shortInterval,
			stateMgr,
			func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				if createSubFunc != nil {
					return createSubFunc(url, nodeConfig, token)
				}
				Fail("createSubscriptionFunc should not be called")
				return "", nil
			},
			&sync.Mutex{},
		).(subRefresher)
	})

	Context("RefreshSubscriptions", func() {
		It("should do nothing when there are no subscriptions", func() {
			refresher.RefreshSubscriptions()
			Expect(stateMgr.State.Subscriptions).To(BeEmpty())
		})

		It("should not update state when subscriptions are up to date", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
				createSubscription(node2Name, sub2, farName, token2),
			}

			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				switch nodeConfig.NodeName {
				case node1Name:
					Expect(token).To(Equal(token1))
					return sub1, nil
				case node2Name:
					Expect(token).To(Equal(token2))
					return sub2, nil
				default:
					Fail("unexpected node: " + nodeConfig.NodeName)
					return "", nil
				}
			}

			refresher.RefreshSubscriptions()
			Expect(stateMgr.State.Subscriptions).To(HaveLen(2))
			Expect(stateMgr.State.Subscriptions[0].URI).To(Equal(sub1))
			Expect(stateMgr.State.Subscriptions[1].URI).To(Equal(sub2))
		})

		It("should update state when subscription URI changed", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				Expect(url).To(Equal(destinationURL))
				Expect(nodeConfig.NodeName).To(Equal(node1Name))
				Expect(token).To(Equal(token1))
				return newSub, nil
			}

			refresher.RefreshSubscriptions()
			Expect(stateMgr.State.Subscriptions).To(HaveLen(1))
			Expect(stateMgr.State.Subscriptions[0].URI).To(Equal(newSub))
		})

		It("should continue refreshing remaining nodes when one verification fails", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
				createSubscription(node2Name, sub2, farName, token2),
			}

			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				if nodeConfig.NodeName == node1Name {
					return "", errors.New("network error")
				}
				return newSub, nil
			}

			refresher.RefreshSubscriptions()
			Expect(stateMgr.State.Subscriptions).To(HaveLen(2))

			subsByNode := map[string]state.Subscription{}
			for _, sub := range stateMgr.State.Subscriptions {
				subsByNode[sub.NodeConfig.NodeName] = sub
			}
			Expect(subsByNode[node1Name].URI).To(Equal(sub1))
			Expect(subsByNode[node2Name].URI).To(Equal(newSub))
		})

		It("should not update state when all verifications fail", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				return "", errors.New("BMC unreachable")
			}

			refresher.RefreshSubscriptions()
			Expect(stateMgr.State.Subscriptions).To(HaveLen(1))
			Expect(stateMgr.State.Subscriptions[0].URI).To(Equal(sub1))
		})
	})

	Context("Start", func() {
		It("should call RefreshSubscriptions periodically and stop when context is canceled", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			var callCount atomic.Int64
			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				callCount.Add(1)
				return sub1, nil
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- refresher.Start(ctx)
			}()

			Eventually(callCount.Load, 10*time.Second, 100*time.Millisecond).
				Should(BeNumerically(">=", 3))
			cancel()

			Eventually(done, time.Second).Should(Receive(BeNil()))
		})

		It("should continue running when individual subscription refresh fails", func() {
			stateMgr.State.Subscriptions = []state.Subscription{
				createSubscription(node1Name, sub1, farName, token1),
			}

			var callCount atomic.Int64
			createSubFunc = func(url string, nodeConfig *state.NodeConfig, token string) (string, error) {
				callCount.Add(1)
				return "", errors.New("temporary error")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- refresher.Start(ctx)
			}()

			Eventually(callCount.Load, time.Second, 100*time.Millisecond).
				Should(BeNumerically(">=", 3))
			cancel()

			Eventually(done, time.Second).Should(Receive(BeNil()))
		})
	})
})
