package node

import (
	"sync"

	state "github.com/0xfelix/redfish-event-listener/pkg/state/v1"
)

type NodeInfoState struct {
	Lock        sync.RWMutex
	Subs        map[string]state.Subscription
	TokenToName map[string]string
}
