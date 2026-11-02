# STATUS

Verified on 2026-11-02 (Go 1.22.2, Linux):

- `go build ./...` OK
- `go vet ./...` OK
- `go test ./...` OK (also `-race` on scheduler, nodes, api, yta)
- `gofmt -l cmd internal` clean

Not verifiable in this environment: Docker image build, live Postgres,
live YouTube/Google/Edge-TTS calls. Run CI (`.github/workflows/ci.yml`)
and a Render deploy to cover those.
