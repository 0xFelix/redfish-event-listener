package redfish

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Watchdog Events", func() {
	DescribeTable("IsAWatchdogResetEvent", func(messageID string, expected bool) {
		Expect(IsWatchdogResetEvent(messageID)).To(Equal(expected))
	},

		Entry("matches exact Dell event", "ASR0001", true),
		Entry("matches Dell event with dotted prefix", "foo.bar.ASR0001", true),
		Entry("does not match different Dell event", "ASR0002", false),
		Entry("matches exact HPE event", "IPMIWatchdogTimerReset", true),
		Entry("matches HPE event with registry/version prefix", "iLOEvents.3.9.IPMIWatchdogTimerReset", true),
		Entry("does not match similar but different HPE event", "IPMIWatchdogTimer", false),
		Entry("does not match unrelated message", "SomethingElse", false),
		Entry("matches exact Supermicro event", "0xc804ff", true),
		Entry("matches Supermicro event with prefix", "SupermicroRegistry.1.2.0xc804ff", true),
		Entry("does not match Supermicro different event", "SupermicroRegistry.1.2.0xc804fa", false),
		Entry("matches exact Lenovo event", "0x806f0823210104ff", true),
		Entry("matches Lenovo event with dotted prefix", "LenovoEventRegistry.2.1.0x806f0823210104ff", true),
		Entry("does not match a different Lenovo event", "LenovoEventRegistry.2.1.0x806f0823210104fa", false),
	)
})
