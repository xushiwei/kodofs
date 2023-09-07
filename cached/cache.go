package cached

import (
	"net/http"

	"github.com/qiniu/x/http/fs/cached/remote"
	"github.com/xushiwei/kodofs"
)

// -----------------------------------------------------------------------------------------

// NewFS creates a cached http.FileSystem to speed up listing directories and accessing file
// contents (optional, only when `cacheFile` is true). If `offline` is true, the cached http.FileSystem
// doesn't access `bkt *kodofs.Bucket`.
func NewFS(local string, bkt *kodofs.Bucket, cacheFile bool, offline ...bool) (fs http.FileSystem, err error) {
	return remote.NewCached(local, bkt, nil, cacheFile, offline...)
}

// -----------------------------------------------------------------------------------------
