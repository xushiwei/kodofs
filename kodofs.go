package kodofs

import (
	"context"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

	xfs "github.com/qiniu/x/http/fs"
	"github.com/xushiwei/kodofs/kodo"
)

var (
	debugNet bool
)

const (
	DbgFlagNetwork = kodo.DbgFlagNetwork
	DbgFlagAll     = kodo.DbgFlagAll
)

func SetDebug(dbgFlags int) {
	debugNet = (dbgFlags & DbgFlagNetwork) != 0
	kodo.SetDebug(dbgFlags)
}

// -----------------------------------------------------------------------------------------

type Credentials kodo.Credentials

func NewCredentials(accessKey, secretKey string) *Credentials {
	return (*Credentials)(kodo.NewCredentials(accessKey, secretKey))
}

// -----------------------------------------------------------------------------------------

type PrepareOpen = func(name string) (ctx context.Context, opener xfs.HttpOpener)

func simplePrepareOpen(name string) (ctx context.Context, opener xfs.HttpOpener) {
	ctx = context.Background()
	return
}

// -----------------------------------------------------------------------------------------

type Bucket struct {
	bkt     *kodo.Bucket
	prepare PrepareOpen
	host    string
}

func (mac *Credentials) NewBucket(bucket string, host string, prepare PrepareOpen) *Bucket {
	if prepare == nil {
		prepare = simplePrepareOpen
	}
	auth := (*kodo.Credentials)(mac)
	bkt := auth.NewBucket(bucket)
	host = strings.TrimSuffix(host, "/")
	return &Bucket{bkt, prepare, host}
}

// Open implements net/http.FileSystem.Open (https://pkg.go.dev/net/http#FileSystem).
func (b *Bucket) Open(name string) (f http.File, err error) {
	ctx, opener := b.prepare(name)
	if name != "/" {
		f, err = opener.Open(ctx, b.host+name)
		if debugNet {
			log.Println("kodofs.Open:", name, "err:", err)
		}
		if err == nil || isIndexPage(name) || !os.IsNotExist(err) {
			return
		}
	}
	fis, err := b.bkt.ReaddirContext(ctx, name)
	if err != nil {
		return
	}
	if len(fis) == 0 {
		return nil, fs.ErrNotExist
	}
	fname := path.Base(name)
	return xfs.Dir(xfs.NewDirInfo(fname), fis), nil
}

func isIndexPage(name string) bool {
	return strings.HasSuffix(name, "/index.html")
}

func (b *Bucket) ReaddirContext(ctx context.Context, dir string) (fis []fs.FileInfo, err error) {
	return b.bkt.ReaddirContext(ctx, dir)
}

// -----------------------------------------------------------------------------------------

func New(accessKey, secretKey string, bucket string, host string, prepare PrepareOpen) *Bucket {
	if debugNet {
		log.Println("kodofs.New:", bucket, host)
	}
	return NewCredentials(accessKey, secretKey).NewBucket(bucket, host, prepare)
}

// -----------------------------------------------------------------------------------------
