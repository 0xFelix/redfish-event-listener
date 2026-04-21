package redfish

import "regexp"

// These are the message IDs that indicate a watchdog
// reset event by the corresponding vendor:
// "ASR0001" 				Dell
// "IPMIWatchdogTimerReset" HPE
// "0xc804ff"               SuperMicro
// "FQXSPWD0004I"           Lenovo
var watchdogResetMessageIDRe = regexp.MustCompile(`ASR0001|IPMIWatchdogTimerReset|0xc804ff|FQXSPWD0004I`)

func IsWatchdogResetEvent(messageID string) bool {
	return watchdogResetMessageIDRe.MatchString(messageID)
}
