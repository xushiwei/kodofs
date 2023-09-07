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

func (mac *Credentials) Upload(ctx context.Context, bucket, name string, r io.Reader, fsize int64) (err error) {
	name = strings.TrimPrefix(name, "/")
	putPolicy := kodo.PutPolicy{
		Scope: bucket + ":" + name,
	}
	upToken := putPolicy.UploadToken((*auth.Credentials)(mac))

	var ret kodo.PutRet
	formUploader := kodo.NewFormUploaderEx(nil, nil)
	return formUploader.Put(ctx, &ret, upToken, name, r, fsize, nil)
}

type WalkFunc = func(path string, info fs.FileInfo, err error) error

func (mac *Credentials) Walk(ctx context.Context, bucket, dir string, fn WalkFunc) (err error) {
	m := kodo.NewBucketManager((*auth.Credentials)(mac), nil)
	dir = strings.TrimPrefix(dir, "/")
	prefix := kodo.ListInputOptionsPrefix(dir)
	limit := kodo.ListInputOptionsLimit(1000)
	marker := ""
	for {
		ret, hasNext, e := m.ListFilesWithContext(ctx, bucket, prefix, limit, kodo.ListInputOptionsMarker(marker))
		if e != nil {
			return e
		}
		for _, item := range ret.Items {
			key := item.Key
			if !strings.HasPrefix(key, "/") {
				key = "/" + key
			}
			fn(key, NewFileInfo(key, item.Fsize), nil)
		}
		if !hasNext {
			break
		}
		marker = ret.Marker
	}
	return
}

// -----------------------------------------------------------------------------------------

// FileInfo describes a single file in an file system.
// It implements fs.FileInfo and fs.DirEntry.
type FileInfo struct {
	name  string
	size  int64
	Mtime time.Time // default use zero time
}

func NewFileInfo(name string, size int64) *FileInfo {
	return &FileInfo{name: name, size: size}
}

func (p *FileInfo) Name() string {
	return path.Base(p.name)
}

func (p *FileInfo) Size() int64 {
	return p.size
}

func (p *FileInfo) Mode() fs.FileMode {
	return fs.ModeIrregular
}

func (p *FileInfo) Type() fs.FileMode {
	return fs.ModeIrregular
}

func (p *FileInfo) ModTime() time.Time {
	return p.Mtime
}

func (p *FileInfo) IsDir() bool {
	return false
}

func (p *FileInfo) Info() (fs.FileInfo, error) {
	return p, nil
}

func (p *FileInfo) Sys() interface{} {
	return nil
}

// -----------------------------------------------------------------------------------------
