// Package localmodel manages ONE optional local AI model (Qwen3 1.7B) on the
// machine MicroFlow itself runs on: safe download of a pinned llama.cpp
// runtime + the GGUF weights, start/stop of the llama-server child process,
// and a small chat client used by the HTTP node (see internal/nodes/
// localmodel_hook.go) so AI requests try the local model first and fall back to
// the existing providers when it is disabled, stopped or failing.
//
// It never runs on Render (or any host that sets RENDER): install/start are
// refused there with a clear message.
package localmodel

import "runtime"

// Pinned runtime release. Every archive's SHA-256 was taken from the GitHub
// release page of ggml-org/llama.cpp tag b11982; the Linux x64 archive was also
// downloaded and re-hashed independently. A mismatch aborts the install.
const llamaTag = "b11982"

const releaseBase = "https://github.com/ggml-org/llama.cpp/releases/download/" + llamaTag + "/"

type runtimeAsset struct {
	Name   string
	SHA256 string
}

var runtimeAssets = map[string]runtimeAsset{
	"windows/amd64": {"llama-" + llamaTag + "-bin-win-cpu-x64.zip", "20cd898fafc269657df8de946b06ef37dcf6b783dfa43d28ac6d187bb6ebfe1e"},
	"windows/arm64": {"llama-" + llamaTag + "-bin-win-cpu-arm64.zip", "6f04f207b9f0a973ae131df726d04d5c8e34de6920b06f8dd5f622790be72b5c"},
	"linux/amd64":   {"llama-" + llamaTag + "-bin-ubuntu-x64.tar.gz", "a7e6cc16d423fc88c204a8d8e646c1d11b1d0de9ec4a56942f6c02936e9bf09c"},
	"linux/arm64":   {"llama-" + llamaTag + "-bin-ubuntu-arm64.tar.gz", "d9e7b51d72a7546b9f6ecb447e2e2e0e4dad4fa096cbba7882ec51a4c836389a"},
	"darwin/arm64":  {"llama-" + llamaTag + "-bin-macos-arm64.tar.gz", "a826c186f464594de3ff3dccec3676dfb35c78643f66e99df6b372feadf84ae6"},
	"darwin/amd64":  {"llama-" + llamaTag + "-bin-macos-x64.tar.gz", "af1d8dfe5e4bb12a71465371addfb8768ab201c26395a32b2baf5329387b524e"},
}

// Official Qwen weights (Apache-2.0), Q8_0 (the only quantisation in the
// official repo), ~1.83 GB. The revision-pinned URL is tried first, then
// "main"; the content hash is verified either way.
const (
	modelFile     = "Qwen3-1.7B-Q8_0.gguf"
	modelSHA256   = "061b54daade076b5d3362dac252678d17da8c68f07560be70818cace6590cb1a"
	modelRevision = "90862c4b9d2787eaed51d12237eafdfe7c5f6077"
	// ModelAlias is the id llama-server reports; it is also what the HTTP
	// node puts in the "model" field of synthesized responses.
	ModelAlias = "qwen3-1.7b-local"
)

var (
	modelMinBytes int64 = 1_500_000_000
	modelMaxBytes int64 = 3_000_000_000
)

// spec is the set of download sources. It is a struct (not constants used
// directly) only so tests can point it at a local httptest server.
type spec struct {
	RuntimeURL    string
	RuntimeName   string
	RuntimeSHA256 string
	ModelURLs     []string
	ModelSHA256   string
	// CheckLinkedETag compares the Hugging Face X-Linked-ETag (the LFS sha256)
	// with ModelSHA256 as a second, independent check.
	CheckLinkedETag bool
	AllowAnyHost    bool // tests only
	RuntimeMinBytes int64
	MinModelBytes   int64
	MaxModelBytes   int64
}

func defaultSpec() (spec, bool) {
	a, ok := runtimeAssets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return spec{}, false
	}
	base := "https://huggingface.co/Qwen/Qwen3-1.7B-GGUF/resolve/"
	return spec{
		RuntimeURL:    releaseBase + a.Name,
		RuntimeName:   a.Name,
		RuntimeSHA256: a.SHA256,
		ModelURLs: []string{
			base + modelRevision + "/" + modelFile,
			base + "main/" + modelFile,
		},
		ModelSHA256:     modelSHA256,
		CheckLinkedETag: true,
		RuntimeMinBytes: 1 << 20,
		MinModelBytes:   modelMinBytes,
		MaxModelBytes:   modelMaxBytes,
	}, true
}
