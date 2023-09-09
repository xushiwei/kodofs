package kodo

import (
	"context"
	"io"
	"strings"

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
