package kodoutil

import (
	"context"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/xushiwei/kodofs/internal/kodo"
	"github.com/xushiwei/kodofs/internal/kodo/auth"
)

// -----------------------------------------------------------------------------------------

type Credentials auth.Credentials

func NewCredentials(accessKey, secretKey string) *Credentials {
	return (*Credentials)(auth.New(accessKey, secretKey))
}

func (mac *Credentials) Upload(ctx context.Context, bucket, name string, r io.Reader, fi fs.FileInfo) (err error) {
	name = strings.TrimPrefix(name, "/")
	putPolicy := kodo.PutPolicy{
		Scope: bucket + ":" + name,
	}
	upToken := putPolicy.UploadToken((*auth.Credentials)(mac))

	var ret kodo.PutRet
	formUploader := kodo.NewFormUploaderEx(nil, nil)
	return formUploader.Put(ctx, &ret, upToken, name, r, fi.Size(), nil)
}

type WalkFunc = func(path string, info fs.FileInfo, err error) error

func (mac *Credentials) Walk(ctx context.Context, bucket, dir string, fn WalkFunc) (err error) {
	m := kodo.NewBucketManager((*auth.Credentials)(mac), nil)
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	marker := ""
	prefix := kodo.ListInputOptionsPrefix(dir)
	for {
		ret, hasNext, e := m.ListFilesWithContext(ctx, bucket, prefix, kodo.ListInputOptionsMarker(marker))
		if e != nil {
			return e
		}
		for _, item := range ret.Items {
			key := item.Key
			if !strings.HasPrefix(key, "/") {
				key = "/" + key
			}
			fn(key, &dataFileInfo{key, item.Fsize}, nil)
		}
		if !hasNext {
			break
		}
		marker = ret.Marker
	}
	return
}

// -----------------------------------------------------------------------------------------

type dataFileInfo struct {
	name string
	size int64
}

func (p *dataFileInfo) Name() string {
	return path.Base(p.name)
}

func (p *dataFileInfo) Size() int64 {
	return p.size
}

func (p *dataFileInfo) Mode() fs.FileMode {
	return 0
}

func (p *dataFileInfo) ModTime() time.Time {
	return time.Time{} // zero time
}

func (p *dataFileInfo) IsDir() bool {
	return false
}

func (p *dataFileInfo) Sys() interface{} {
	return nil
}

// -----------------------------------------------------------------------------------------
