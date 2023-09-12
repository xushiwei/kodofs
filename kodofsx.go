package kodofs

import (
	"context"
	"fmt"
	"net/http"

	"github.com/qiniu/x/http/fsx"
	"github.com/xushiwei/kodofs/kodoutil"
)

// -----------------------------------------------------------------------------------------

const (
	Scheme = kodoutil.Scheme
)

func init() {
	fsx.Register(Scheme, Open)
}

// Open a kodofs file system by url in form of "kodo:<bucketName>?<token>".
func Open(ctx context.Context, url string) (_ http.FileSystem, _ fsx.Closer, err error) {
	bucket, ak, sk, err := kodoutil.Parse(url)
	if err != nil {
		return
	}
	host, ok := hosts[bucket]
	if !ok {
		err = fmt.Errorf("host of bucket `%s` not found, please call kodofs.Register", bucket)
		return
	}
	return New(ak, sk, bucket, host, nil), nil, nil
}

// -----------------------------------------------------------------------------------------

var (
	hosts = make(map[string]string, 8)
)

// Register registers (bucket, host) pairs for kodofs.
func Register(bucketHostPairs ...string) {
	for i, n := 0, len(bucketHostPairs); i < n; i += 2 {
		bucket, host := bucketHostPairs[i], bucketHostPairs[i+1]
		hosts[bucket] = host
	}
}

// -----------------------------------------------------------------------------------------
