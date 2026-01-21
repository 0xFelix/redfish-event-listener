package far_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFarTemplateSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Far Template Controller Suite")
}
