package runner

import (
	"context"
	"testing"
	"time"
)

type fakeStaleStore struct {
	ids       []string
	finalized []string
	gotCutoff time.Time
}

func (f *fakeStaleStore) ListStaleExecutionIDs(_ context.Context, before time.Time, _ int) ([]string, error) {
	f.gotCutoff = before
	return f.ids, nil
}

func (f *fakeStaleStore) FinalizeStaleExecution(_ context.Context, id string) (bool, error) {
	f.finalized = append(f.finalized, id)
	return true, nil
}

func TestReapSkipsLiveAndClosesRest(t *testing.T) {
	st := &fakeStaleStore{ids: []string{"live", "dead1", "dead2"}}
	isLive := func(id string) bool { return id == "live" }
	before := time.Now()
	n, err := ReapStaleExecutions(context.Background(), st, isLive, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(st.finalized) != 2 || st.finalized[0] != "dead1" || st.finalized[1] != "dead2" {
		t.Fatalf("closed=%d finalized=%v, want dead1,dead2 only", n, st.finalized)
	}
	// The grace period must be applied to the query cutoff.
	if st.gotCutoff.After(before.Add(-3*time.Minute + time.Second)) {
		t.Fatalf("cutoff %v does not respect the 3m grace", st.gotCutoff)
	}
}
