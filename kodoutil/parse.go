package kodoutil

import (
	"io/fs"
	"strings"

	"github.com/qiniu/x/token/protected"
)

const (
	Scheme = "kodo"
)

// -----------------------------------------------------------------------------------------

// url = "kodo:<bucketName>?<token>"
func Parse(url string) (bucket, ak, sk string, err error) {
	url = strings.TrimPrefix(url, Scheme+":")
	parts := strings.SplitN(url, "?", 2)
	if len(parts) != 2 {
		err = fs.ErrPermission
		return
	}
	params, err := protected.Decode(parts[1])
	if err != nil {
		return
	}
	ak, sk = params.Get("ak"), params.Get("sk")
	if ak == "" || sk == "" {
		err = fs.ErrPermission
		return
	}
	bucket = parts[0]
	return
}

// -----------------------------------------------------------------------------------------
