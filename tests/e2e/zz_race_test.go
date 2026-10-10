//go:build e2e

package e2e

import (
	"os/exec"
	"strings"
	"testing"
)

// This file sorts last so the check runs after every other scenario has exercised the instances.
// Container logs survive restarts, so races seen before a crash scenario are still reported.
func TestNoDataRacesWereReported(t *testing.T) {
	if env("E2E_RACE", "") != "1" {
		t.Skip("set E2E_RACE=1 with the race-instrumented stack (compose.race.yaml) to enable this check")
	}
	for _, name := range instances {
		out, err := exec.Command("docker", "logs", container(name)).CombinedOutput()
		if err != nil {
			t.Fatalf("logs of %s: %v", name, err)
		}
		if i := strings.Index(string(out), "WARNING: DATA RACE"); i >= 0 {
			end := i + 1500
			if end > len(out) {
				end = len(out)
			}
			t.Errorf("%s reported a data race:\n%s", name, out[i:end])
		}
	}
}
