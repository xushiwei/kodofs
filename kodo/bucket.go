package kodo

import (
	"context"
	"io/fs"
	"log"
	"strings"
	"time"

	xfs "github.com/qiniu/x/http/fs"
	"github.com/xushiwei/kodofs/internal/kodo"
	"github.com/xushiwei/kodofs/internal/kodo/auth"
)

var (
	debugNet bool
)

const (
	DbgFlagNetwork = 1 << iota
	DbgFlagAll     = DbgFlagNetwork
)

func SetDebug(dbgFlags int) {
	debugNet = (dbgFlags & DbgFlagNetwork) != 0
}

// -----------------------------------------------------------------------------------------

type Bucket struct {
	mac    *auth.Credentials
	m      *kodo.BucketManager
	bucket string
}

// NewBucket opens a Bucket object.
func (mac *Credentials) NewBucket(bucket string) *Bucket {
	auth := (*auth.Credentials)(mac)
	m := kodo.NewBucketManager(auth, nil)
	return &Bucket{auth, m, bucket}
}

func (b *Bucket) Credentials() *Credentials {
	return (*Credentials)(b.mac)
}

func (b *Bucket) Name() string {
	return b.bucket
}

// -----------------------------------------------------------------------------------------

type WalkFunc = func(path string, info fs.FileInfo, err error) error

func (b *Bucket) WalkContext(ctx context.Context, dir string, fn WalkFunc) (err error) {
	m, bucket := b.m, b.bucket
	if dir == "/" {
		dir = ""
	} else {
		dir = strings.TrimPrefix(dir, "/")
		if !strings.HasSuffix(dir, "/") {
			dir += "/"
		}
	}
	prefix := kodo.ListInputOptionsPrefix(dir)
	limit := kodo.ListInputOptionsLimit(1000)
	marker := ""
	for {
		ret, hasNext, e := m.ListFilesWithContext(ctx, bucket, prefix, limit, kodo.ListInputOptionsMarker(marker))
		if e != nil {
			return e
		}
		if debugNet {
			log.Println("kodo.List:", dir, "hasNext:", hasNext, "items:", len(ret.Items))
		}
		for _, item := range ret.Items {
			key := item.Key
			name := key[len(dir):]
			fn("/"+key, xfs.NewFileInfo(name, item.Fsize), nil)
		}
		if !hasNext {
			break
		}
		marker = ret.Marker
	}
	return
}

// -----------------------------------------------------------------------------------------

func (b *Bucket) ReaddirContext(ctx context.Context, dir string) (fis []fs.FileInfo, err error) {
	m, bucket := b.m, b.bucket
	if dir == "/" {
		dir = ""
	} else {
		dir = strings.TrimPrefix(dir, "/")
		if !strings.HasSuffix(dir, "/") {
			dir += "/"
		}
	}
	delimiter := kodo.ListInputOptionsDelimiter("/")
	prefix := kodo.ListInputOptionsPrefix(dir)
	limit := kodo.ListInputOptionsLimit(1000)
	marker := ""
	fis = make([]fs.FileInfo, 0, 64)
	for {
		ret, hasNext, e := m.ListFilesWithContext(ctx, bucket, prefix, delimiter, limit, kodo.ListInputOptionsMarker(marker))
		if e != nil {
			return nil, e
		}
		if debugNet {
			log.Println("kodo.List:", dir, "hasNext:", hasNext, "items:", len(ret.Items), "commonPrefixes:", ret.CommonPrefixes)
		}
		for _, item := range ret.Items {
			key := item.Key
			name := key[len(dir):]
			fi := xfs.NewFileInfo(name, item.Fsize)
			fi.Mtime = fromPutTime(item.PutTime)
			fis = append(fis, fi)
		}
		for _, key := range ret.CommonPrefixes {
			name := key[len(dir) : len(key)-1]
			fis = append(fis, xfs.NewDirInfo(name))
		}
		if !hasNext {
			break
		}
		marker = ret.Marker
	}
	return
}

func fromPutTime(putTime int64) time.Time {
	return time.Unix(0, putTime*100)
}

// -----------------------------------------------------------------------------------------
