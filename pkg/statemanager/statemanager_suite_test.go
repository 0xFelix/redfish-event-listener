package statemanager

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStateManager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "StateManager Suite")
}
