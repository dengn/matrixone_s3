package objectstoragefs

import (
	"context"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lni/vfs"
)

var _ vfs.FS = (*Store)(nil)

func name(p string) string {
	p = path.Clean("/" + strings.TrimPrefix(p, "/"))
	// Dragonboat converts its data path to an absolute path. The bucket's
	// logical namespace must not include the caller's machine/work directory.
	if i := strings.Index(p, "/bucket-log"); i >= 0 && (len(p) == i+len("/bucket-log") || p[i+len("/bucket-log")] == '/') {
		return p[i:]
	}
	return p
}
func (s *Store) dirtyName(p string) { s.dirty[name(p)] = true }
func (s *Store) wrap(p string, f vfs.File, e error) (vfs.File, error) {
	if e != nil {
		return nil, e
	}
	wrapped := &file{File: f, store: s, path: name(p), inode: s.files["wal:"+name(p)].Inode}
	s.handles[wrapped] = true
	return wrapped, nil
}
func (s *Store) Create(p string) (vfs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	f, e := s.mem.Create(name(p))
	if e == nil {
		s.dirtyName(p)
		n := s.files["wal:"+name(p)]
		n.Inode = uuid.NewString()
		s.files["wal:"+name(p)] = n
	}
	return s.wrap(p, f, e)
}
func (s *Store) Open(p string, opts ...vfs.OpenOption) (vfs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, e := s.mem.Open(name(p))
	f, e = s.wrap(p, f, e)
	if e != nil {
		return nil, e
	}
	for _, o := range opts {
		o.Apply(f)
	}
	return f, nil
}
func (s *Store) OpenDir(p string) (vfs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, e := s.mem.OpenDir(name(p))
	return s.wrap(p, f, e)
}
func (s *Store) OpenForAppend(p string) (vfs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, e := s.mem.OpenForAppend(name(p))
	return s.wrap(p, f, e)
}
func (s *Store) MkdirAll(p string, mode os.FileMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	if e := s.mem.MkdirAll(name(p), mode); e != nil {
		return e
	}
	for n := name(p); n != "/"; n = path.Dir(n) {
		s.dirtyName(n)
		s.files["wal:"+n] = Node{Dir: true}
	}
	return nil
}
func (s *Store) Link(a, b string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	e := s.mem.Link(name(a), name(b))
	if e == nil {
		s.dirtyName(a)
		s.dirtyName(b)
		s.files["wal:"+name(b)] = s.files["wal:"+name(a)]
	}
	return e
}
func (s *Store) Rename(a, b string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	e := s.mem.Rename(name(a), name(b))
	if e == nil {
		s.dirtyName(a)
		s.dirtyName(b)
		s.renameNamespace(name(a), name(b))
	}
	return e
}
func (s *Store) Remove(p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	e := s.mem.Remove(name(p))
	if e == nil {
		s.dirtyName(p)
		delete(s.files, "wal:"+name(p))
	}
	return e
}
func (s *Store) RemoveAll(p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	e := s.mem.RemoveAll(name(p))
	if e == nil {
		for k := range s.files {
			if strings.HasPrefix(k, "wal:"+name(p)+"/") || k == "wal:"+name(p) {
				s.dirtyName(strings.TrimPrefix(k, "wal:"))
				delete(s.files, k)
			}
		}
		s.dirtyName(p)
	}
	return e
}
func (s *Store) ReuseForWrite(a, b string) (vfs.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	f, e := s.mem.ReuseForWrite(name(a), name(b))
	if e == nil {
		s.dirtyName(a)
		s.dirtyName(b)
		s.renameNamespace(name(a), name(b))
	}
	return s.wrap(b, f, e)
}
func (s *Store) Lock(p string) (io.Closer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	return s.mem.Lock(name(p))
}
func (s *Store) List(p string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.List(name(p))
}
func (s *Store) Stat(p string) (os.FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem.Stat(name(p))
}
func (s *Store) PathBase(p string) string       { return path.Base(p) }
func (s *Store) PathDir(p string) string        { return path.Dir(p) }
func (s *Store) PathJoin(elem ...string) string { return path.Join(elem...) }
func (s *Store) GetDiskUsage(p string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{TotalBytes: 1 << 40, AvailBytes: 1 << 40}, nil
}

type file struct {
	vfs.File
	store *Store
	path  string
	inode string
}

func (s *Store) renameNamespace(a, b string) {
	updates := make(map[string]Node)
	for key, node := range s.files {
		p := strings.TrimPrefix(key, "wal:")
		if key == "wal:"+a || strings.HasPrefix(key, "wal:"+a+"/") {
			updates["wal:"+b+strings.TrimPrefix(p, a)] = node
			delete(s.files, key)
		}
	}
	for key, node := range updates {
		s.files[key] = node
	}
	for f := range s.handles {
		if f.path == a || strings.HasPrefix(f.path, a+"/") {
			f.path = b + strings.TrimPrefix(f.path, a)
		}
	}
}
func (f *file) Close() error {
	s := f.store
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handles, f)
	return f.File.Close()
}

func (f *file) Write(b []byte) (int, error) {
	s := f.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return 0, e
	}
	n, e := f.File.Write(b)
	if n > 0 {
		s.dirtyName(f.path)
	}
	return n, e
}
func (f *file) WriteAt(b []byte, off int64) (int, error) {
	s := f.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return 0, e
	}
	n, e := f.File.WriteAt(b, off)
	if n > 0 {
		s.dirtyName(f.path)
	}
	return n, e
}
func (f *file) Sync() error {
	s := f.store
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, c := context.WithTimeout(context.Background(), 30*time.Second)
	defer c()
	if e := s.check(); e != nil {
		return e
	}
	info, e := f.File.Stat()
	if e != nil {
		return e
	}
	if !info.IsDir() {
		current, ok := s.files["wal:"+f.path]
		if !ok || current.Inode != f.inode {
			return nil
		}
		contents, e := s.mem.Open(f.path)
		if e != nil {
			return e
		}
		data, e := io.ReadAll(contents)
		_ = contents.Close()
		if e != nil {
			return e
		}
		n, e := s.encode(ctx, data)
		if e != nil {
			return e
		}
		n.Inode = f.inode
		for key, node := range s.files {
			if strings.HasPrefix(key, "wal:") && !node.Dir && node.Inode == f.inode {
				s.files[key] = n
			}
		}
	}
	return s.flush(ctx)
}
func (s *Store) flush(ctx context.Context) error {
	if e := s.check(); e != nil {
		return e
	}
	// Only file.Sync replaces durable WAL bytes. Publishing another file,
	// directory or native object must never capture a half-written WAL record.
	if e := s.publish(ctx); e != nil {
		return e
	}
	clear(s.dirty)
	return nil
}
