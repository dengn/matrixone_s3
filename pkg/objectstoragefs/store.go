// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

// Package objectstoragefs implements an experimental single-writer filesystem
// whose authoritative state, including Raft WALs, lives in an S3 bucket.
package objectstoragefs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"golang.org/x/sync/singleflight"
)

const ChunkSize = 256 << 10

var ErrFenced = errors.New("bucket filesystem writer fenced")
var ErrWriterBusy = errors.New("bucket database already has an active writer")

type Config struct {
	LiveUpdates  bool   `json:"live_updates,omitempty"`
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	Mode         string `json:"mode"`
	Snapshot     string `json:"snapshot,omitempty"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}
type Node struct {
	Inode  string   `json:"inode,omitempty"`
	Dir    bool     `json:"dir,omitempty"`
	Size   int64    `json:"size,omitempty"`
	Chunks []string `json:"chunks,omitempty"`
}
type Manifest struct {
	LiveLSN uint64          `json:"live_lsn,omitempty"`
	Format  int             `json:"format"`
	Files   map[string]Node `json:"files"`
}
type Head struct {
	Format         int    `json:"format"`
	Owner          string `json:"owner"`
	Epoch          uint64 `json:"epoch"`
	LeaseUntil     int64  `json:"lease_until"`
	Version        string `json:"version"`
	VisibleVersion string `json:"visible_version"`
}
type Store struct {
	livePrepareMu  sync.Mutex
	livePrepared   []*preparedLiveBatch
	liveFlights    singleflight.Group
	liveConsumer   func(context.Context, []logtail.TableLogtail) error
	liveLSN        uint64
	mu             sync.Mutex
	endpoints      sync.Map
	config         Config
	client         *s3.Client
	mem            *vfs.MemFS
	files          map[string]Node
	dirty          map[string]bool
	cache          map[string][]byte
	overlay        map[string][]byte
	known          map[string]bool
	cacheBytes     int
	handles        map[*file]bool
	head           Head
	manifestDigest [32]byte
	etag, owner    string
	fenced         bool
	closed         bool
	stop           chan struct{}
	done           chan struct{}
}

var active atomic.Pointer[Store]

func Current() *Store { return active.Load() }
func Configure(ctx context.Context, cfg Config) (*Store, error) {
	s, e := Open(ctx, cfg)
	if e == nil {
		active.Store(s)
	}
	return s, e
}

// Runtime transports are deliberately not restored from the database manifest.
// Each bucket attachment must route exclusively to its own local native roles.
func (s *Store) RegisterEndpoint(role, id, kind, addr string) {
	s.endpoints.Store(role+"/"+id+"/"+kind, addr)
}
func (s *Store) Endpoint(role, id, kind string) string {
	if value, ok := s.endpoints.Load(role + "/" + id + "/" + kind); ok {
		return value.(string)
	}
	return ""
}
func (s *Store) ReadOnly() bool  { return s.config.Mode == "reader" }
func (s *Store) Version() string { s.mu.Lock(); defer s.mu.Unlock(); return s.head.Version }

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" || cfg.Prefix == "" {
		return nil, fmt.Errorf("bucket and a nonempty database prefix are required")
	}
	if cfg.Mode != "writer" && cfg.Mode != "reader" {
		return nil, fmt.Errorf("mode must be writer or reader")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.LeaseSeconds == 0 {
		cfg.LeaseSeconds = 60
	}
	if cfg.LeaseSeconds < 3 {
		return nil, fmt.Errorf("lease must be at least three seconds")
	}
	if cfg.AccessKey == "" {
		cfg.AccessKey = os.Getenv("MO_BUCKET_ACCESS_KEY")
		cfg.SecretKey = os.Getenv("MO_BUCKET_SECRET_KEY")
	}
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKey != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, e := awsconfig.LoadDefaultConfig(ctx, options...)
	if e != nil {
		return nil, e
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	s := &Store{config: cfg, client: client, mem: vfs.NewMem(), files: make(map[string]Node), dirty: make(map[string]bool), cache: make(map[string][]byte), overlay: make(map[string][]byte), known: make(map[string]bool), handles: make(map[*file]bool), stop: make(chan struct{}), done: make(chan struct{})}
	data, etag, e := s.get(ctx, "head.json")
	if e != nil && !missing(e) {
		return nil, e
	}
	if e == nil {
		if e = json.Unmarshal(data, &s.head); e != nil {
			return nil, e
		}
		if s.head.Format != 1 {
			return nil, fmt.Errorf("unsupported bucket database format")
		}
		s.etag = etag
	}
	if cfg.Mode == "writer" {
		if s.head.Owner != "" && s.head.LeaseUntil > time.Now().UnixMilli() {
			return nil, ErrWriterBusy
		}
		s.owner = uuid.NewString()
		s.head.Owner = s.owner
		s.head.Epoch++
		s.head.Format = 1
		s.head.LeaseUntil = time.Now().Add(time.Duration(cfg.LeaseSeconds) * time.Second).UnixMilli()
	}
	version := s.head.Version
	if cfg.Mode == "reader" && cfg.Snapshot != "" {
		version = cfg.Snapshot
	} else if cfg.Mode == "reader" && s.head.VisibleVersion != "" {
		version = s.head.VisibleVersion
	}
	if version != "" {
		data, _, e = s.get(ctx, "manifests/"+version+".json")
		if e != nil {
			return nil, e
		}
		var m Manifest
		if e = json.Unmarshal(data, &m); e != nil {
			return nil, e
		}
		if m.Format != 1 || m.Files == nil {
			return nil, fmt.Errorf("invalid bucket manifest")
		}
		s.files = m.Files
		s.liveLSN = m.LiveLSN
		s.manifestDigest = sha256.Sum256(data)
		for key, node := range m.Files {
			if strings.HasPrefix(key, "wal:") {
				if !node.Dir && node.Inode == "" {
					node.Inode = uuid.NewString()
					s.files[key] = node
				}
				canonical := "wal:" + name(strings.TrimPrefix(key, "wal:"))
				if canonical != key {
					delete(s.files, key)
					s.files[canonical] = node
				}
			}
		}
		s.head.Version = version
		for _, node := range s.files {
			for _, hash := range node.Chunks {
				s.known[hash] = true
			}
		}
	}
	if e = s.restoreWAL(ctx); e != nil {
		return nil, e
	}
	if cfg.Mode == "writer" {
		// Recover the immutable state before acquiring the lease. A slow remote
		// restore cannot let an already-acquired lease expire mid-startup. The
		// original head ETag still arbitrates any competing owner/update.
		s.head.LeaseUntil = time.Now().Add(time.Duration(cfg.LeaseSeconds) * time.Second).UnixMilli()
		if e = s.saveHead(ctx); e != nil {
			return nil, e
		}
		go s.renew()
	} else {
		close(s.done)
	}
	return s, nil
}
func missing(e error) bool {
	var api smithy.APIError
	return errors.As(e, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound")
}
func (s *Store) key(k string) string { return strings.Trim(s.config.Prefix, "/") + "/" + k }
func (s *Store) get(ctx context.Context, k string) ([]byte, string, error) {
	o, e := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.key(k))})
	if e != nil {
		return nil, "", e
	}
	defer o.Body.Close()
	b, e := io.ReadAll(o.Body)
	return b, aws.ToString(o.ETag), e
}
func (s *Store) put(ctx context.Context, k string, b []byte) error {
	_, e := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.key(k)), Body: bytes.NewReader(b)})
	return e
}
func (s *Store) saveHead(ctx context.Context) error {
	if s.fenced {
		return ErrFenced
	}
	b, e := json.Marshal(s.head)
	if e != nil {
		return e
	}
	input := &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.key("head.json")), Body: bytes.NewReader(b)}
	if s.etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(s.etag)
	}
	o, e := s.client.PutObject(ctx, input)
	if e != nil {
		s.fenced = true
		return fmt.Errorf("%w: conditional head update failed: %v", ErrFenced, e)
	}
	s.etag = aws.ToString(o.ETag)
	return nil
}
func (s *Store) renew() {
	defer close(s.done)
	ticker := time.NewTicker(time.Duration(s.config.LeaseSeconds) * time.Second / 3)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			if !s.closed && !s.fenced {
				s.head.LeaseUntil = time.Now().Add(time.Duration(s.config.LeaseSeconds) * time.Second).UnixMilli()
				ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
				_ = s.saveHead(ctx)
				c()
			}
			s.mu.Unlock()
		}
	}
}
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stop)
	s.mu.Unlock()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ReadOnly() || s.fenced {
		return nil
	}
	s.head.Owner = ""
	s.head.LeaseUntil = 0
	ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	return s.saveHead(ctx)
}
func (s *Store) check() error {
	if s.closed {
		return os.ErrClosed
	}
	if s.fenced {
		return ErrFenced
	}
	return nil
}
func (s *Store) chunk(ctx context.Context, hash string) ([]byte, error) {
	if b, ok := s.overlay[hash]; ok {
		return b, nil
	}
	if b, ok := s.cache[hash]; ok {
		return b, nil
	}
	b, _, e := s.get(ctx, "chunks/"+hash)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != hash {
		return nil, fmt.Errorf("bucket chunk checksum mismatch")
	}
	if s.cacheBytes+len(b) > 128<<20 {
		clear(s.cache)
		s.cacheBytes = 0
	}
	s.cache[hash] = b
	s.cacheBytes += len(b)
	return b, nil
}
func (s *Store) bytes(ctx context.Context, n Node, off, size int64) ([]byte, error) {
	if off < 0 || off > n.Size {
		return nil, io.ErrUnexpectedEOF
	}
	if size < 0 {
		size = n.Size - off
	}
	if size < 0 || size > n.Size-off {
		return nil, io.ErrUnexpectedEOF
	}
	out := make([]byte, size)
	for pos := int64(0); pos < size; {
		index := (off + pos) / ChunkSize
		inner := (off + pos) % ChunkSize
		if int(index) >= len(n.Chunks) {
			return nil, io.ErrUnexpectedEOF
		}
		chunk, e := s.chunk(ctx, n.Chunks[index])
		if e != nil {
			return nil, e
		}
		if inner >= int64(len(chunk)) {
			return nil, io.ErrUnexpectedEOF
		}
		amount := min(size-pos, int64(len(chunk))-inner)
		copy(out[pos:pos+amount], chunk[inner:inner+amount])
		pos += amount
	}
	return out, nil
}
func (s *Store) encode(ctx context.Context, b []byte) (Node, error) {
	n := Node{Size: int64(len(b))}
	for off := 0; off < len(b); off += ChunkSize {
		chunk := b[off:min(off+ChunkSize, len(b))]
		h := sha256.Sum256(chunk)
		hash := hex.EncodeToString(h[:])
		if !s.known[hash] {
			if s.ReadOnly() {
				s.overlay[hash] = bytes.Clone(chunk)
			} else if e := s.put(ctx, "chunks/"+hash, chunk); e != nil {
				return Node{}, e
			}
			s.known[hash] = true
		}
		n.Chunks = append(n.Chunks, hash)
	}
	return n, nil
}
func (s *Store) publish(ctx context.Context) error {
	if e := s.check(); e != nil {
		return e
	}
	if s.ReadOnly() {
		return nil
	}
	b, e := json.Marshal(Manifest{Format: 1, Files: s.files, LiveLSN: s.liveLSN})
	if e != nil {
		return e
	}
	digest := sha256.Sum256(b)
	if digest != s.manifestDigest || s.head.Version == "" {
		version := uuid.NewString()
		if e = s.put(ctx, "manifests/"+version+".json", b); e != nil {
			return e
		}
		s.head.Version = version
	}
	// Even a no-change fsync must conditionally validate ownership. Reusing an
	// immutable manifest must not let a paused/fenced writer report success.
	s.head.LeaseUntil = time.Now().Add(time.Duration(s.config.LeaseSeconds) * time.Second).UnixMilli()
	if e = s.saveHead(ctx); e != nil {
		return e
	}
	s.manifestDigest = digest
	return nil
}

// PublishVisible marks the fully committed native engine state as reader-visible.
// Raft/WAL fsync has already made the transaction durable before this is called.
func (s *Store) PublishVisible(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ReadOnly() {
		return nil
	}
	if e := s.flush(ctx); e != nil {
		return e
	}
	s.head.VisibleVersion = s.head.Version
	return s.saveHead(ctx)
}
func (s *Store) restoreWAL(ctx context.Context) error {
	keys := make([]string, 0)
	for k := range s.files {
		if strings.HasPrefix(k, "wal:") {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) < len(keys[j]) })
	inodes := make(map[string]string)
	for _, key := range keys {
		n := s.files[key]
		p := strings.TrimPrefix(key, "wal:")
		if n.Dir {
			if e := s.mem.MkdirAll(p, 0755); e != nil {
				return e
			}
			continue
		}
		if e := s.mem.MkdirAll(path.Dir(p), 0755); e != nil {
			return e
		}
		if previous, ok := inodes[n.Inode]; ok && n.Inode != "" {
			if e := s.mem.Link(previous, p); e != nil {
				return e
			}
			continue
		}
		f, e := s.mem.Create(p)
		if e != nil {
			return e
		}
		inodes[n.Inode] = p
		b, e := s.bytes(ctx, n, 0, -1)
		if e == nil {
			_, e = f.Write(b)
		}
		_ = f.Close()
		if e != nil {
			return e
		}
	}
	return nil
}

// Shared object methods use the same atomic manifest as the WAL filesystem.
func (s *Store) ReadObject(ctx context.Context, name string, off, size int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.files["data:"+name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return s.bytes(ctx, n, off, size)
}
func (s *Store) ObjectSize(name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.files["data:"+name]
	if !ok {
		return 0, os.ErrNotExist
	}
	return n.Size, nil
}
func (s *Store) WriteObject(ctx context.Context, name string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	if _, ok := s.files["data:"+name]; ok {
		return os.ErrExist
	}
	n, e := s.encode(ctx, b)
	if e != nil {
		return e
	}
	s.files["data:"+name] = n
	if e = s.flush(ctx); e != nil {
		delete(s.files, "data:"+name)
	}
	return e
}

// ReplaceObject atomically replaces persistent control-plane metadata.
func (s *Store) ReplaceObject(ctx context.Context, name string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	n, e := s.encode(ctx, b)
	if e != nil {
		return e
	}
	old, existed := s.files["data:"+name]
	s.files["data:"+name] = n
	if e = s.flush(ctx); e != nil {
		if existed {
			s.files["data:"+name] = old
		} else {
			delete(s.files, "data:"+name)
		}
	}
	return e
}
func (s *Store) DeleteObjects(ctx context.Context, names []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	previous := make(map[string]Node)
	for _, n := range names {
		key := "data:" + n
		if node, ok := s.files[key]; ok {
			previous[key] = node
		}
		delete(s.files, key)
	}
	if err := s.flush(ctx); err != nil {
		for key, node := range previous {
			s.files[key] = node
		}
		return err
	}
	return nil
}
func (s *Store) ObjectNames(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for k := range s.files {
		if strings.HasPrefix(k, "data:"+prefix) {
			names = append(names, strings.TrimPrefix(k, "data:"))
		}
	}
	sort.Strings(names)
	return names
}
