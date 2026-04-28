package far

import (
	"context"
	"log"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/0xfelix/redfish-event-listener/pkg/statemanager"
)

const defaultSubscriptionRefreshInterval = 10 * time.Minute

type subscriptionRefresher struct {
	destinationURL string
	interval       time.Duration
	stateManager   statemanager.StateManager
	createSub      CreateSubscriptionFunc
	reconcilerLock *sync.Mutex
}

var (
	_ manager.Runnable               = &subscriptionRefresher{}
	_ manager.LeaderElectionRunnable = &subscriptionRefresher{}
)

func NewSubscriptionRefresher(
	destinationURL string,
	interval time.Duration,
	stateMgr statemanager.StateManager,
	createSub CreateSubscriptionFunc,
	reconcilerLock *sync.Mutex,
) manager.Runnable {
	if interval == 0 {
		interval = defaultSubscriptionRefreshInterval
	}
	return &subscriptionRefresher{
		destinationURL: destinationURL,
		interval:       interval,
		stateManager:   stateMgr,
		createSub:      createSub,
		reconcilerLock: reconcilerLock,
	}
}

func (s *subscriptionRefresher) NeedLeaderElection() bool {
	return true
}

func (s *subscriptionRefresher) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.RefreshSubscriptions()
		}
	}
}

// RefreshSubscriptions verifies each stored subscription is still registered on the BMC.
// If a subscription was removed it will be recreated (CreateSubscription is idempotent).
func (s *subscriptionRefresher) RefreshSubscriptions() {
	s.reconcilerLock.Lock()
	defer s.reconcilerLock.Unlock()

	currentSubs := s.stateManager.GetSubscriptions()
	if len(currentSubs) == 0 {
		return
	}

	updated := false
	for i := range currentSubs {
		sub := &currentSubs[i]
		subURI, err := s.createSub(s.destinationURL, &sub.NodeConfig, sub.Token)
		if err != nil {
			log.Printf("Failed to verify subscription for node %q: %v", sub.NodeConfig.NodeName, err)
			continue
		}
		if sub.URI != subURI {
			log.Printf("Subscription URI updated for node %q: %s -> %s", sub.NodeConfig.NodeName, sub.URI, subURI)
			sub.URI = subURI
			updated = true
		}
	}

	if updated {
		s.stateManager.SetSubscriptions(currentSubs)
	}
}
