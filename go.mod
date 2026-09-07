module microflow

go 1.22

// VERIFIED: `go build ./...`, `go vet ./...`, and `go test ./... -race` all
// pass in-sandbox against these exact resolved versions (see STATUS.md).
// The golang.org/x/* replaces point at official GitHub mirrors, and the
// gopkg.in/yaml.v2, yaml.v3, check.v1, errgo.v2 replaces point at
// ./third_party (real upstream source, vendored here because this sandbox's
// network allowlist can reach github.com/codeload.github.com but not
// gopkg.in directly). All of this is optional on a machine with normal
// internet access -- `go mod tidy` there would resolve the same modules
// via the real module proxy without needing ./third_party at all -- but
// leaving it in place is harmless and keeps the build reproducible here too.
require (
	github.com/dop251/goja v0.0.0-20240220182346-e401ed450204 // pure-Go JS engine, sandboxed Code-node execution
	github.com/jackc/pgx/v5 v5.5.5 // pure-Go Postgres driver (works with Neon/Supabase/Render/self-hosted)
)

require (
	github.com/dlclark/regexp2 v1.7.0 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.5.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace golang.org/x/text => github.com/golang/text v0.14.0

replace golang.org/x/crypto => github.com/golang/crypto v0.17.0

replace golang.org/x/sync => github.com/golang/sync v0.1.0

replace golang.org/x/sys => github.com/golang/sys v0.15.0

replace golang.org/x/net => github.com/golang/net v0.17.0

replace golang.org/x/mod => github.com/golang/mod v0.14.0

replace golang.org/x/tools => github.com/golang/tools v0.16.0

replace gopkg.in/yaml.v2 => ./third_party/gopkg.in_yaml.v2

replace gopkg.in/yaml.v3 => ./third_party/gopkg.in_yaml.v3

replace gopkg.in/check.v1 => ./third_party/gopkg.in_check.v1

replace gopkg.in/errgo.v2 => ./third_party/gopkg.in_errgo.v2

replace golang.org/x/term => github.com/golang/term v0.13.0
