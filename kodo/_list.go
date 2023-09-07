package kodo

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/xushiwei/kodofs/internal/kodo"
)

// -----------------------------------------------------------------------------------------

// ListOptions sets options for listing blobs via Bucket.List.
type ListOptions struct {
	// Prefix indicates that only blobs with a key starting with this prefix
	// should be returned.
	Prefix string
	// Delimiter sets the delimiter used to define a hierarchical namespace,
	// like a filesystem with "directories". It is highly recommended that you
	// use "" or "/" as the Delimiter. Other values should work through this API,
	// but service UIs generally assume "/".
	//
	// An empty delimiter means that the bucket is treated as a single flat
	// namespace.
	//
	// A non-empty delimiter means that any result with the delimiter in its key
	// after Prefix is stripped will be returned with ListObject.IsDir = true,
	// ListObject.Key truncated after the delimiter, and zero values for other
	// ListObject fields. These results represent "directories". Multiple results
	// in a "directory" are returned as a single result.
	Delimiter string

	PageToken string
}

// ListObject represents a single blob returned from List.
type ListObject struct {
	// Key is the key for this blob.
	Key string
	// ModTime is the time the blob was last modified.
	ModTime time.Time
	// Size is the size of the blob's content in bytes.
	Size int64
	// IsDir indicates that this result represents a "directory" in the
	// hierarchical namespace, ending in ListOptions.Delimiter. Key can be
	// passed as ListOptions.Prefix to list items in the "directory".
	// Fields other than Key and IsDir will not be set if IsDir is true.
	IsDir bool
}

// ListIterator iterates over List results.
type ListIterator struct {
	b          *Bucket
	opts       *listPagedOptions
	page       *listPage
	nextIdx    int
	isLastPage bool
}

// Next returns a *ListObject for the next blob. It returns (nil, io.EOF) if
// there are no more.
func (i *ListIterator) Next(ctx context.Context) (_ *ListObject, err error) {
	if i.page != nil {
		// We've already got a page of results.
		if i.nextIdx < len(i.page.Objects) {
			// Next object is in the page; return it.
			obj := i.page.Objects[i.nextIdx]
			i.nextIdx++

			mtime := fromPutTime(obj.PutTime)
			key := obj.Key
			isDir := strings.HasSuffix(key, "/")
			if isDir && key != "/" {
				key = key[:len(key)-1]
			}
			return &ListObject{key, mtime, obj.Fsize, isDir}, nil
		}
		if i.isLastPage {
			// Done with current page, and there are no more; return io.EOF.
			return nil, io.EOF
		}
		// We need to load the next page.
		i.opts.PageToken = i.page.NextPageToken
	}
	// Loading a new page.
	p, isLastPage, err := i.b.listPaged(ctx, i.opts)
	if err != nil {
		return
	}
	i.page = p
	i.nextIdx = 0
	i.isLastPage = isLastPage
	return i.Next(ctx)
}

// -----------------------------------------------------------------------------------------

// List returns a ListIterator that can be used to iterate over blobs in a
// bucket, in lexicographical order of UTF-8 encoded keys. The underlying
// implementation fetches results in pages.
//
// A nil ListOptions is treated the same as the zero value.
//
// List is not guaranteed to include all recently-written blobs;
// some services are only eventually consistent.
func (b *Bucket) List(opts *ListOptions) *ListIterator {
	const (
		pageSize = 1000
	)
	if opts == nil {
		opts = &ListOptions{}
	}
	dopts := &listPagedOptions{
		Prefix:    opts.Prefix,
		Delimiter: opts.Delimiter,
		PageSize:  pageSize,
	}
	return &ListIterator{b: b, opts: dopts}
}

// -----------------------------------------------------------------------------------------

// listPage represents a page of results return from ListPaged.
type listPage struct {
	// Objects is the slice of objects found. If ListOptions.PageSize > 0,
	// it should have at most ListOptions.PageSize entries.
	//
	// Objects should be returned in lexicographical order of UTF-8 encoded keys,
	// including across pages. I.e., all objects returned from a ListPage request
	// made using a PageToken from a previous ListPage request's NextPageToken
	// should have Key >= the Key for all objects from the previous request.
	Objects []kodo.ListItem

	NextPageToken string
}

// listPagedOptions sets options for listing objects in the bucket.
type listPagedOptions struct {
	// Prefix indicates that only results with the given prefix should be
	// returned.
	Prefix string
	// Delimiter sets the delimiter used to define a hierarchical namespace,
	// like a filesystem with "directories".
	//
	// An empty delimiter means that the bucket is treated as a single flat
	// namespace.
	//
	// A non-empty delimiter means that any result with the delimiter in its key
	// after Prefix is stripped will be returned with ListObject.IsDir = true,
	// ListObject.Key truncated after the delimiter, and zero values for other
	// ListObject fields. These results represent "directories". Multiple results
	// in a "directory" are returned as a single result.
	Delimiter string
	// PageSize sets the maximum number of objects to be returned.
	// 0 means no maximum; driver implementations should choose a reasonable
	// max. It is guaranteed to be >= 0.
	PageSize int
	// PageToken may be filled in with the NextPageToken from a previous
	// ListPaged call.
	PageToken string
}

func (b *Bucket) listPaged(ctx context.Context, opts *listPagedOptions) (_ *listPage, isLastPage bool, err error) {
	prefix := kodo.ListInputOptionsPrefix(opts.Prefix)
	limit := kodo.ListInputOptionsLimit(opts.PageSize)
	delimiter := kodo.ListInputOptionsDelimiter(opts.Delimiter)
	marker := kodo.ListInputOptionsMarker(opts.PageToken)
	ret, hasNext, err := b.m.ListFilesWithContext(ctx, b.bucket, prefix, limit, delimiter, marker)
	if err != nil {
		return
	}
	return &listPage{ret.Items, ret.Marker}, !hasNext, nil
}

// -----------------------------------------------------------------------------------------
