package fileservice

import (
	"bytes"
	"context"
	"io"
	"iter"
	"os"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectstoragefs"
)

// BucketFS maps immutable native objects into the atomic bucket manifest shared
// with the WAL filesystem. A reader uses a local overlay and cannot publish it.
type BucketFS struct {
	name      string
	namespace string
	store     *objectstoragefs.Store
}

var _ FileService = (*BucketFS)(nil)

func NewBucketFS(n string) (FileService, error) {
	s := objectstoragefs.Current()
	if s == nil {
		return nil, moerr.NewInvalidInputNoCtx("BUCKET requires --bucket-config")
	}
	namespace := ""
	if strings.EqualFold(n, "LOCAL") {
		namespace = "__local__/"
	}
	return &BucketFS{name: n, namespace: namespace, store: s}, nil
}
func (b *BucketFS) Name() string          { return b.name }
func (b *BucketFS) Close(context.Context) {}
func (b *BucketFS) file(p string) (string, error) {
	parsed, e := ParsePathAtService(p, b.name)
	if e != nil {
		return "", e
	}
	return b.namespace + parsed.File, nil
}

func (b *BucketFS) Replace(ctx context.Context, v IOVector) error {
	p, e := b.file(v.FilePath)
	if e != nil {
		return e
	}
	data, e := io.ReadAll(newIOEntriesReader(ctx, v.Entries))
	if e != nil {
		return e
	}
	return b.store.ReplaceObject(ctx, p, data)
}
func (b *BucketFS) Write(ctx context.Context, v IOVector) error {
	p, e := b.file(v.FilePath)
	if e != nil {
		return e
	}
	if p == "" {
		return moerr.NewFileNotFoundNoCtx(p)
	}
	if _, e = b.store.ObjectSize(p); e == nil {
		return moerr.NewFileAlreadyExistsNoCtx(p)
	}
	data, e := io.ReadAll(newIOEntriesReader(ctx, v.Entries))
	if e != nil {
		return e
	}
	e = b.store.WriteObject(ctx, p, data)
	if os.IsExist(e) {
		return moerr.NewFileAlreadyExistsNoCtx(p)
	}
	return e
}
func (b *BucketFS) Read(ctx context.Context, v *IOVector) (err error) {
	p, e := b.file(v.FilePath)
	if e != nil {
		return e
	}
	defer func() {
		if err != nil {
			v.ReleaseReadResultOnError()
		}
	}()
	for i := range v.Entries {
		entry := &v.Entries[i]
		if entry.done {
			continue
		}
		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(p)
		}
		data, e := b.store.ReadObject(ctx, p, entry.Offset, entry.Size)
		if os.IsNotExist(e) {
			return moerr.NewFileNotFoundNoCtx(p)
		}
		if e != nil {
			return e
		}
		entry.Size = int64(len(data))
		if entry.WriterForRead != nil {
			if _, e = entry.WriterForRead.Write(data); e != nil {
				return e
			}
		} else if entry.ReadCloserForRead != nil {
			*entry.ReadCloserForRead = io.NopCloser(bytes.NewReader(data))
		} else {
			entry.Data = data
		}
		if e = entry.setCachedData(ctx, DefaultCacheDataAllocator()); e != nil {
			return e
		}
		entry.done = true
	}
	return nil
}
func (b *BucketFS) ReadCache(context.Context, *IOVector) error { return nil }
func (b *BucketFS) Delete(ctx context.Context, ps ...string) error {
	names := make([]string, len(ps))
	for i, p := range ps {
		n, e := b.file(p)
		if e != nil {
			return e
		}
		names[i] = n
	}
	return b.store.DeleteObjects(ctx, names)
}
func (b *BucketFS) List(ctx context.Context, dir string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		p, e := b.file(dir)
		if e != nil {
			yield(nil, e)
			return
		}
		prefix := strings.Trim(p, "/")
		if prefix != "" {
			prefix += "/"
		}
		seen := make(map[string]bool)
		for _, n := range b.store.ObjectNames(prefix) {
			if e := ctx.Err(); e != nil {
				yield(nil, e)
				return
			}
			rest := strings.TrimPrefix(n, prefix)
			first, _, isDir := strings.Cut(rest, "/")
			if seen[first] {
				continue
			}
			seen[first] = true
			size, _ := b.store.ObjectSize(n)
			if !yield(&DirEntry{Name: first, IsDir: isDir, Size: size}, nil) {
				return
			}
		}
	}
}
func (b *BucketFS) StatFile(ctx context.Context, p string) (*DirEntry, error) {
	n, e := b.file(p)
	if e != nil {
		return nil, e
	}
	size, e := b.store.ObjectSize(n)
	if os.IsNotExist(e) {
		return nil, moerr.NewFileNotFoundNoCtx(n)
	}
	if e != nil {
		return nil, e
	}
	return &DirEntry{Name: path.Base(n), Size: size}, nil
}
func (b *BucketFS) PrefetchFile(context.Context, string) error { return nil }
func (b *BucketFS) Cost() *CostAttr                            { return &CostAttr{List: CostHigh} }
