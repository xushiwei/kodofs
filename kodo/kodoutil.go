package kodo

import (
	"context"
	"io"
	"io/fs"
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

// -----------------------------------------------------------------------------------------

// FileInfo describes a single file in an file system.
// It implements fs.FileInfo and fs.DirEntry.
type FileInfo struct {
	name  string
	size  int64
	Mtime time.Time
}

func NewFileInfo(name string, size int64) *FileInfo {
	return &FileInfo{name: name, size: size}
}

func (p *FileInfo) Name() string {
	return p.name
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

// DirInfo describes a single directory in an file system.
// It implements fs.FileInfo and io.Closer and Stat().
type DirInfo struct {
	name string
}

func NewDirInfo(name string) *DirInfo {
	return &DirInfo{name}
}

func (p *DirInfo) Name() string {
	return p.name
}

func (p *DirInfo) Size() int64 {
	return 0
}

func (p *DirInfo) Mode() fs.FileMode {
	return fs.ModeIrregular | fs.ModeDir
}

func (p *DirInfo) ModTime() time.Time {
	return time.Now()
}

func (p *DirInfo) IsDir() bool {
	return true
}

func (p *DirInfo) Sys() interface{} {
	return nil
}

func (p *DirInfo) Stat() (fs.FileInfo, error) {
	return p, nil
}

func (p *DirInfo) Close() error {
	return nil
}

// -----------------------------------------------------------------------------------------
