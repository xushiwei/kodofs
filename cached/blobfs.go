package cached

import (
	"context"
	"encoding/base64"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	xfs "github.com/qiniu/x/http/fs"
	"github.com/qiniu/x/http/fs/cached"
	xdir "github.com/qiniu/x/http/fs/cached/dir"
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

const (
	dirListCacheFile = ".bktls.cache"
)

func checkDirCached(dir string) fs.FileInfo {
	cacheFile := filepath.Join(dir, dirListCacheFile)
	fi, err := os.Lstat(cacheFile)
	if err != nil {
		fi = nil
	}
	return fi
}

func touchDirCached(dir string) error {
	cacheFile := filepath.Join(dir, dirListCacheFile)
	return os.WriteFile(cacheFile, nil, 0666)
}

func writeStubFile(localFile string, fi fs.FileInfo) error {
	if fi.IsDir() {
		return os.Mkdir(localFile, 0755)
	}
	fi = &fileInfoRemote{fi}
	b := xdir.BytesFileInfo(fi)
	dest := base64.URLEncoding.EncodeToString(b)
	// don't need to restore mtime for symlink (saved by BytesFileInfo)
	return os.Symlink(dest, localFile)
}

func readStubFile(localFile string, fi fs.FileInfo) fs.FileInfo {
	dest, e1 := os.Readlink(localFile)
	if e1 == nil {
		if b, e2 := base64.URLEncoding.DecodeString(dest); e2 == nil {
			if ret, e3 := xdir.FileInfoFrom(b); e3 == nil {
				return ret
			}
		}
	}
	return fi
}

func isRemote(fi fs.FileInfo) bool {
	return (fi.Mode() & fs.ModeSymlink) != 0
}

// -----------------------------------------------------------------------------------------

type readdirFile interface {
	Readdir(n int) ([]fs.FileInfo, error)
}

func readdir(f http.File) ([]fs.FileInfo, error) {
	if r, ok := f.(readdirFile); ok {
		return r.Readdir(-1)
	}
	log.Panicln("[FATAL] Readdir notimpl")
	return nil, fs.ErrInvalid
}

// -----------------------------------------------------------------------------------------

type objFile struct {
	http.File
	localFile string
	notify    NotifyFile
}

func (p *objFile) Close() error {
	file := p.File
	if err := cached.DownloadFile(p.localFile, file); err == nil {
		fi, _ := file.Stat()
		os.Chtimes(p.localFile, time.Time{}, fi.ModTime()) // restore mtime
		if notify := p.notify; notify != nil {
			name := file.(interface{ FullName() string }).FullName()
			notify.NotifyFile(context.Background(), name, fi)
		}
	} else {
		log.Println("[WARN] Cache file failed:", err)
	}
	return file.Close()
}

type fileInfoRemote struct {
	fs.FileInfo
}

func (p *fileInfoRemote) Mode() fs.FileMode {
	return p.FileInfo.Mode() | cached.ModeRemote
}

// -----------------------------------------------------------------------------------------

type remote struct {
	bucket    http.FileSystem
	notify    NotifyFile
	cacheFile bool
}

func (p *remote) ReaddirAll(localDir string, dir *os.File, offline bool) (fis []fs.FileInfo, err error) {
	if fis, err = dir.Readdir(-1); err != nil {
		return
	}
	n := 0
	for _, fi := range fis {
		name := fi.Name()
		if name == dirListCacheFile { // skip dir cache file
			continue
		}
		if isRemote(fi) {
			if offline {
				continue
			}
			localFile := filepath.Join(localDir, name)
			fi = readStubFile(localFile, fi)
		}
		fis[n] = fi
		n++
	}
	return fis[:n], nil
}

func (p *remote) Lstat(localFile string) (fi fs.FileInfo, err error) {
	fi, err = os.Lstat(localFile)
	if err != nil {
		return
	}
	if fi.IsDir() {
		if checkDirCached(localFile) == nil { // no dir cache
			fi = &fileInfoRemote{fi}
		}
	} else if isRemote(fi) {
		fi = readStubFile(localFile, fi)
	}
	return
}

func (p *remote) SyncLstat(local string, name string) (fi fs.FileInfo, err error) {
	return nil, os.ErrNotExist
}

func (p *remote) SyncOpen(local string, name string) (f http.File, err error) {
	if name == "/" {
		name = "."
	} else {
		name = strings.TrimPrefix(name, "/")
	}
	o, err := p.bucket.Open(name)
	if err != nil {
		log.Printf(`[ERROR] bucket.Open("%s"): %v\n`, name, err)
		return
	}
	if debugNet {
		log.Println("[INFO] ==> bucket.Open", name)
	}
	if o.(interface{ IsDir() bool }).IsDir() {
		fis, e := readdir(o)
		if e != nil {
			log.Printf(`[ERROR] Readdir("%s"): %v\n`, name, e)
			return nil, e
		}
		if debugNet {
			log.Println("[INFO] ==> Readdir", name, "-", len(fis), "items")
		}
		go func() {
			nError := 0
			base := filepath.Join(local, name)
			for _, fi := range fis {
				itemFile := base + "/" + fi.Name()
				if writeStubFile(itemFile, fi) != nil {
					nError++
				}
			}
			if nError == 0 {
				touchDirCached(base)
			} else {
				log.Printf("[WARN] writeStubFile fail (%d errors)", nError)
			}
		}()
		return cached.Dir(o, fis), nil
	}
	localFile := filepath.Join(local, name)
	return &objFile{xfs.SequenceFile(name, o), localFile, p.notify}, nil
}

func (p *remote) Init(local string, offline bool) {
}

// -----------------------------------------------------------------------------------------

type NotifyFile interface {
	NotifyFile(ctx context.Context, name string, fi fs.FileInfo)
}

func NewRemote(bucket http.FileSystem, notifyOrNil NotifyFile, cacheFile bool) (ret cached.Remote, err error) {
	return &remote{bucket, notifyOrNil, cacheFile}, nil
}

func NewFS(local string, bucket http.FileSystem, notifyOrNil NotifyFile, cacheFile bool, offline ...bool) (fs http.FileSystem, err error) {
	r, err := NewRemote(bucket, notifyOrNil, cacheFile)
	if err != nil {
		return
	}
	return cached.New(local, r, offline...), nil
}

// -----------------------------------------------------------------------------------------
