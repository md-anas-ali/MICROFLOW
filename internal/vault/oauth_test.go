// UNVERIFIED IN SANDBOX: written but never run here. See ../../STATUS.md.
// This test deliberately only exercises needsRefresh (pure time-math,
// no network) -- Resolve/refresh themselves need a real Store + real
// Google endpoint and are explicitly a local-machine test.
package vault

import (
	"strconv"
	"testing"
	"time"
)

func TestNeedsRefreshEmptyExpiry(t *testing.T) {
	if !needsRefresh("") {
		t.Error("empty expiresAt should always mean refresh")
	}
}

func TestNeedsRefreshMalformedExpiry(t *testing.T) {
	if !needsRefresh("not-a-number") {
		t.Error("malformed expiresAt should be treated as needing refresh")
	}
}

func TestNeedsRefreshFarFuture(t *testing.T) {
	future := strconv.FormatInt(time.Now().Add(1*time.Hour).Unix(), 10)
	if needsRefresh(future) {
		t.Error("token expiring in an hour should not need refresh yet")
	}
}

func TestNeedsRefreshWithinLeeway(t *testing.T) {
	soon := strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10)
	if !needsRefresh(soon) {
		t.Error("token expiring in 30s (inside the 2-minute leeway) should need refresh")
	}
}

func TestNeedsRefreshAlreadyExpired(t *testing.T) {
	past := strconv.FormatInt(time.Now().Add(-1*time.Hour).Unix(), 10)
	if !needsRefresh(past) {
		t.Error("already-expired token should need refresh")
	}
}
