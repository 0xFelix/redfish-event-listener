package node

import (
	"sync"
)

type NodeConfig struct {
	NodeName string `json:"nodeName"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	Insecure bool   `json:"insecure,omitempty"`
}

type NodeInfo struct {
	NodeConfig     NodeConfig
	SubscriptionID string
	FarObjName     string
	Token          string
}

type NodeInfoState struct {
	Lock        sync.RWMutex
	Infos       map[string]NodeInfo
	TokenToName map[string]string
}
