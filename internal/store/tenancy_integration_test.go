package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"microflow/internal/model"
	"microflow/internal/tenant"
)

// Integration test: runs only when MICROFLOW_TEST_DATABASE_URL is set.
func TestTenancyIntegration(t *testing.T) {
	url := os.Getenv("MICROFLOW_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("MICROFLOW_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	st, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	// ApplySchema twice: must be idempotent.
	for i := 0; i < 2; i++ {
		if err := st.ApplySchema(ctx, string(schema)); err != nil {
			t.Fatalf("schema pass %d: %v", i, err)
		}
	}
	if _, err := st.GetService(ctx, tenant.DefaultID); err != nil {
		t.Fatalf("default service missing: %v", err)
	}
	a := &tenant.Service{ID: "a-1", Name: "A"}
	b := &tenant.Service{ID: "b-1", Name: "B"}
	for _, s := range []*tenant.Service{a, b} {
		_ = st.DeleteService(ctx, s.ID)
		if err := st.CreateService(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	wfA := &model.Workflow{ID: "wf-a", Name: "wa", ServiceID: "a-1"}
	wfB := &model.Workflow{ID: "wf-b", Name: "wb", ServiceID: "b-1"}
	for _, w := range []*model.Workflow{wfA, wfB} {
		if err := st.SaveWorkflow(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	// service_id immutable across re-save
	wfA2 := &model.Workflow{ID: "wf-a", Name: "wa2", ServiceID: "b-1"}
	if err := st.SaveWorkflow(ctx, wfA2); err != nil {
		t.Fatal(err)
	}
	got, _ := st.LoadWorkflow(ctx, "wf-a")
	if got.ServiceID != "a-1" {
		t.Fatalf("service moved: %s", got.ServiceID)
	}
	la, _ := st.ListWorkflowsByService(ctx, "a-1")
	if len(la) != 1 || la[0].ID != "wf-a" {
		t.Fatalf("isolation broken: %+v", la)
	}
	// env round trip via raw ciphertext
	if err := st.PutServiceEnv(ctx, "a-1", "K", []byte("x"), true); err != nil {
		t.Fatal(err)
	}
	m, _ := st.AllServiceEnvCiphertext(ctx, "b-1")
	if len(m) != 0 {
		t.Fatal("env leaked across services")
	}
	// run-all single-active guard
	_ = st.UpdateRunAllJob(ctx, "j0", "cancelled", []byte("[]"), "")
	if err := st.CreateRunAllJob(ctx, "j1", []string{"a-1"}, false); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRunAllJob(ctx, "j2", []string{"a-1"}, false); !errors.Is(err, ErrRunAllAlreadyActive) {
		t.Fatalf("want ErrRunAllAlreadyActive got %v", err)
	}
	_ = st.UpdateRunAllJob(ctx, "j1", "success", []byte("[]"), "")
	// delete service cascades
	if err := st.DeleteService(ctx, "a-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadWorkflow(ctx, "wf-a"); err == nil {
		t.Fatal("workflow survived service delete")
	}
	_ = st.DeleteService(ctx, "b-1")
}
