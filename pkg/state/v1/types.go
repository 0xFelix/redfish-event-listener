package v1

type State struct {
	Version       string         `json:"version"`
	Subscriptions []Subscription `json:"subscriptions,omitempty"`
}

type Subscription struct {
	NodeConfig      NodeConfig `json:"nodeConfig"`
	URI             string     `json:"uri"`
	FarTemplateName string     `json:"farTemplateName"`
	Token           string     `json:"token"`
}

type NodeConfig struct {
	NodeName string `json:"nodeName"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
	Insecure bool   `json:"insecure,omitempty"`
}

const VersionV1 = "v1"
