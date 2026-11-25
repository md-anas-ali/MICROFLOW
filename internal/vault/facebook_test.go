package vault

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type memAccounts struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *memAccounts) GetEncryptedAccount(_ context.Context, a string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.m[a]; ok {
		return b, nil
	}
	return nil, errors.New("none")
}
func (s *memAccounts) PutEncryptedAccount(_ context.Context, a string, c []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string][]byte{}
	}
	s.m[a] = c
	return nil
}
func (s *memAccounts) DeleteEncryptedAccount(_ context.Context, a string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, a)
	return nil
}
func (s *memAccounts) AccountCredentialMeta(_ context.Context, a string) (time.Time, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[a]
	return time.Now(), ok, nil
}

func fakeGraph(t *testing.T, pages string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth/access_token"):
			r.ParseForm()
			if r.PostForm.Get("grant_type") == "fb_exchange_token" {
				if r.PostForm.Get("fb_exchange_token") != "SHORT" {
					t.Errorf("exchange got wrong token")
				}
				w.Write([]byte(`{"access_token":"LONG","expires_in":5184000}`))
				return
			}
			if r.PostForm.Get("code") != "CODE" {
				t.Errorf("bad code")
			}
			w.Write([]byte(`{"access_token":"SHORT"}`))
		case strings.HasSuffix(r.URL.Path, "/me/accounts"):
			if r.Header.Get("Authorization") != "Bearer LONG" {
				w.WriteHeader(400)
				w.Write([]byte(`{"error":{"message":"expired","code":190}}`))
				return
			}
			w.Write([]byte(pages))
		case strings.HasSuffix(r.URL.Path, "/me"):
			w.Write([]byte(`{"id":"U1","name":"Rahim"}`))
		case strings.HasSuffix(r.URL.Path, "/debug_token"):
			w.Write([]byte(`{"data":{"is_valid":true}}`))
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
}

const twoPages = `{"data":[{"id":"P1","name":"One","access_token":"PT1","instagram_business_account":{"id":"IG1","username":"one_ig"}},{"id":"P2","name":"Two","access_token":"PT2"}]}`

func TestFacebookConnectAndPageAccess(t *testing.T) {
	srv := fakeGraph(t, twoPages)
	defer srv.Close()
	old := facebookGraph
	facebookGraph = srv.URL
	defer func() { facebookGraph = old }()

	app := NewFacebookApp("APP", "SECRET", "https://x.example/api/oauth/facebook/callback", "")
	authURL, _ := app.AuthURL("svc1")
	if !strings.Contains(authURL, "pages_manage_posts") || strings.Contains(authURL, "SECRET") {
		t.Fatalf("auth url wrong: %s", authURL)
	}
	state := strings.Split(strings.Split(authURL, "state=")[1], "&")[0]
	state, _ = urlUnescape(state)
	if id, ok := PeekStateServiceID(state); !ok || id != "svc1" {
		t.Fatalf("peek: %q %v", id, ok)
	}
	if _, _, err := app.Exchange(context.Background(), "CODE", state+"x"); err == nil {
		t.Fatal("tampered state accepted")
	}
	msvc, secrets, err := app.Exchange(context.Background(), "CODE", state)
	if err != nil || msvc != "svc1" {
		t.Fatalf("exchange: %v %q", err, msvc)
	}

	v, _ := New(&dummyStore{}, "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
	acc := NewFacebookAccount(v.NewAccountVault(&memAccounts{}))
	ctx := context.Background()
	if acc.Connected(ctx) {
		t.Fatal("should start disconnected")
	}
	if _, _, err := acc.PageAccess(ctx, "P1"); err == nil {
		t.Fatal("expected not-connected error")
	}
	if err := acc.Put(ctx, secrets); err != nil {
		t.Fatal(err)
	}
	if tok, ig, err := acc.PageAccess(ctx, "P1"); err != nil || tok != "PT1" || ig != "IG1" {
		t.Fatalf("P1: %q %q %v", tok, ig, err)
	}
	if tok, ig, err := acc.PageAccess(ctx, " P2 "); err != nil || tok != "PT2" || ig != "" {
		t.Fatalf("P2: %q %q %v", tok, ig, err)
	}
	if _, _, err := acc.PageAccess(ctx, "NOPE"); err == nil || !strings.Contains(err.Error(), "Rahim") {
		t.Fatalf("unknown page error: %v", err)
	}
	st, _ := acc.Status(ctx, app)
	if !st.Connected || st.NeedsReconnect || len(st.Pages) != 2 || st.Pages[0].Instagram != "one_ig" {
		t.Fatalf("status: %+v", st)
	}
	for _, p := range st.Pages { // status must never carry tokens
		if strings.Contains(p.Name+p.ID+p.Instagram, "PT") {
			t.Fatal("token leaked in status")
		}
	}
	if err := acc.Disconnect(ctx); err != nil || acc.Connected(ctx) {
		t.Fatal("disconnect failed")
	}
}

func TestFacebookNewPageFoundLater(t *testing.T) {
	srv := fakeGraph(t, `{"data":[{"id":"P1","name":"One","access_token":"PT1"},{"id":"P3","name":"Three","access_token":"PT3"}]}`)
	defer srv.Close()
	old := facebookGraph
	facebookGraph = srv.URL
	defer func() { facebookGraph = old }()
	app := NewFacebookApp("APP", "SECRET", "https://x/cb", "")
	v, _ := New(&dummyStore{}, "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
	acc := NewFacebookAccount(v.NewAccountVault(&memAccounts{}))
	acc.SetRefresher(func(context.Context) *FacebookApp { return app })
	// stored: only P1; user token valid ("LONG")
	acc.Put(context.Background(), map[string]string{"userToken": "LONG", "name": "R",
		"pages": `[{"id":"P1","name":"One","token":"PT1"}]`})
	if tok, _, err := acc.PageAccess(context.Background(), "P3"); err != nil || tok != "PT3" {
		t.Fatalf("P3: %q %v", tok, err)
	}
	// now saved: works even if the user token is dead
	acc.Put(context.Background(), map[string]string{"userToken": "DEAD", "name": "R",
		"pages": `[{"id":"P3","name":"Three","token":"PT3"}]`})
	if tok, _, err := acc.PageAccess(context.Background(), "P3"); err != nil || tok != "PT3" {
		t.Fatalf("P3 after user token death: %q %v", tok, err)
	}
	if _, _, err := acc.PageAccess(context.Background(), "P9"); err == nil || !strings.Contains(err.Error(), "reconnect") {
		t.Fatalf("expected reconnect error: %v", err)
	}
}

func TestFacebookAppFromEnv(t *testing.T) {
	_, missing := FacebookOAuthAppFromEnv(func(string) string { return "" }, "")
	if len(missing) != 3 {
		t.Fatalf("missing: %v", missing)
	}
	env := map[string]string{"FB_APP_ID": "a", "FB_APP_SECRET": "b", "HOST_URL": "https://h.example/"}
	app, missing := FacebookOAuthAppFromEnv(func(k string) string { return env[k] }, "https://ignored.example")
	if app == nil || missing != nil || app.RedirectURL != "https://h.example"+FacebookOAuthCallbackPath {
		t.Fatalf("%+v %v", app, missing)
	}
	// no HOST_URL at all: derived from the request address
	only := map[string]string{"FB_APP_ID": "a", "FB_APP_SECRET": "b"}
	app, missing = FacebookOAuthAppFromEnv(func(k string) string { return only[k] }, "https://auto.example/")
	if app == nil || missing != nil || app.RedirectURL != "https://auto.example"+FacebookOAuthCallbackPath {
		t.Fatalf("auto: %+v %v", app, missing)
	}
}

type dummyStore struct{}

func (dummyStore) GetEncrypted(context.Context, string, string) ([]byte, error) {
	return nil, ErrCredentialNotFound
}
func (dummyStore) PutEncrypted(context.Context, string, string, []byte) error { return nil }

func urlUnescape(s string) (string, error) { return url.QueryUnescape(s) }

func TestFacebookAutoAssignAZ(t *testing.T) {
	v, _ := New(&dummyStore{}, "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=")
	acc := NewFacebookAccount(v.NewAccountVault(&memAccounts{}))
	ctx := context.Background()
	svcs := []string{"s1", "s2", "s3", "s4"} // oldest first
	acc.SetServiceLister(func(context.Context) ([]string, error) { return svcs, nil })
	acc.Put(ctx, map[string]string{"name": "R", "pages": `[{"id":"Z","name":"Zeta","token":"t"},{"id":"A","name":"alpha","token":"t"},{"id":"B","name":"Beta","token":"t"}]`})
	got := acc.Assignments(ctx)
	if len(got) != 3 || got["s1"] != "A" || got["s2"] != "B" || got["s3"] != "Z" || got["s4"] != "" {
		t.Fatalf("3 pages / 4 services: %v (s4 must get nothing)", got)
	}
	// more Pages than Services: extras unused
	svcs = []string{"s1", "s2"}
	if got = acc.Assignments(ctx); len(got) != 2 || got["s1"] != "A" || got["s2"] != "B" {
		t.Fatalf("2 services: %v", got)
	}
	// re-sorted every time: a new Page that sorts first shifts everyone
	acc.Put(ctx, map[string]string{"name": "R", "pages": `[{"id":"Z","name":"Zeta","token":"t"},{"id":"A","name":"alpha","token":"t"},{"id":"AA","name":"Aardvark","token":"t"}]`})
	if got = acc.Assignments(ctx); got["s1"] != "AA" || got["s2"] != "A" {
		t.Fatalf("re-sort: %v", got)
	}
	if acc.ServicePage(ctx, "s2") != "A" || acc.ServicePage(ctx, "nobody") != "" {
		t.Fatal("ServicePage wrong")
	}
}
