package nodes

import (
	"encoding/base64"
	"os"
)

func base64URLEncode(b []byte) string {
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(b)
}

func openForUpload(path string) (*os.File, error) {
	return os.Open(path)
}
