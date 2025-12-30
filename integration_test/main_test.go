package integration

import (
	"os"
	"testing"
)

var Infra *InfraStructure

func TestMain(m *testing.M) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") // Podman: disable Ryuk
	var cleanup func()
	Infra, cleanup = SetupInfra()
	defer cleanup()

	code := m.Run()
	os.Exit(code)
}
