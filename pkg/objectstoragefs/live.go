// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package objectstoragefs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"slices"
	"strings"
)

func (s *Store) LiveUpdates() bool { return s.config.LiveUpdates }
func liveName(lsn uint64) string   { return fmt.Sprintf("data:__live__/wal/%020d", lsn) }

// ExportLiveWAL shares native transaction commands, never SQL strings. It runs
// before the native WAL completion signal so acknowledged commits include it.
func (s *Store) ExportLiveWAL(ctx context.Context, lsn uint64, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if lsn <= s.liveLSN {
		return nil
	}
	n, err := s.encode(ctx, payload)
	if err != nil {
		return err
	}
	key := liveName(lsn)
	old := s.liveLSN
	s.files[key] = n
	s.liveLSN = lsn
	if err = s.flush(ctx); err != nil {
		delete(s.files, key)
		s.liveLSN = old
	}
	return err
}

// preparedLiveBatch is immutable after publication. Prefetch never imports
// source file references or changes native query/transaction visibility.
type preparedLiveBatch struct {
	version     string
	after, upto uint64
	manifest    Manifest
	payloads    [][]byte
}

func (s *Store) cachedLiveBatch(version string, after uint64) *preparedLiveBatch {
	s.livePrepareMu.Lock()
	defer s.livePrepareMu.Unlock()
	for _, batch := range s.livePrepared {
		if batch.version == version && batch.after <= after && after <= batch.upto {
			return batch
		}
	}
	return nil
}

// liveChunk releases the filesystem lock during remote I/O. Background work
// cannot hold up a cached foreground read while waiting for the network.
func (s *Store) liveChunk(ctx context.Context, hash string) ([]byte, error) {
	s.mu.Lock()
	b, ok := s.cache[hash]
	if !ok {
		b, ok = s.overlay[hash]
	}
	s.mu.Unlock()
	if ok {
		return b, nil
	}
	b, _, err := s.get(ctx, "chunks/"+hash)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != hash || len(b) > ChunkSize {
		return nil, fmt.Errorf("bucket chunk checksum or size mismatch")
	}
	s.mu.Lock()
	if s.cacheBytes+len(b) > 128<<20 {
		clear(s.cache)
		s.cacheBytes = 0
	}
	if _, exists := s.cache[hash]; !exists {
		s.cache[hash] = b
		s.cacheBytes += len(b)
	}
	s.mu.Unlock()
	return b, nil
}

// PrepareLiveBatch fetches and validates a contiguous native WAL batch without
// admitting it to TAE or importing its file references. Two prepared versions
// are retained, each with at most 64 MiB of transaction payloads. Singleflight
// shares an in-flight fetch with queries requesting the same source frontier.
func (s *Store) PrepareLiveBatch(ctx context.Context, version string, after uint64) (uint64, error) {
	batch, err := s.prepareLiveBatch(ctx, version, after)
	if err != nil {
		return after, err
	}
	return batch.upto, nil
}

func (s *Store) prepareLiveBatch(ctx context.Context, version string, after uint64) (*preparedLiveBatch, error) {
	if !s.ReadOnly() || !s.LiveUpdates() {
		return nil, fmt.Errorf("live reader required")
	}
	if batch := s.cachedLiveBatch(version, after); batch != nil {
		return batch, nil
	}
	key := fmt.Sprintf("%s/%d", version, after)
	result := s.liveFlights.DoChan(key, func() (any, error) {
		if batch := s.cachedLiveBatch(version, after); batch != nil {
			return batch, nil
		}
		b, _, err := s.get(ctx, "manifests/"+version+".json")
		if err != nil {
			return nil, err
		}
		var m Manifest
		if err = json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		if m.Format != 1 || m.Files == nil || m.LiveLSN < after {
			return nil, fmt.Errorf("invalid or regressing live manifest: source %d applied %d", m.LiveLSN, after)
		}
		if m.LiveLSN-after > 4096 {
			return nil, fmt.Errorf("live batch exceeds 4096 records; reattach to current snapshot")
		}
		batch := &preparedLiveBatch{version: version, after: after, upto: m.LiveLSN, manifest: m}
		total := int64(0)
		for lsn := after + 1; lsn <= m.LiveLSN; lsn++ {
			node, ok := m.Files[liveName(lsn)]
			if !ok {
				return nil, fmt.Errorf("missing live WAL %d", lsn)
			}
			if node.Size < 0 || node.Size > (64<<20)-total {
				return nil, fmt.Errorf("live batch exceeds 64 MiB; reattach to current snapshot")
			}
			if node.Dir || len(node.Chunks) != int((node.Size+ChunkSize-1)/ChunkSize) {
				return nil, fmt.Errorf("invalid native WAL chunk layout at %d", lsn)
			}
			total += node.Size
			payload := make([]byte, 0, int(node.Size))
			for i, hash := range node.Chunks {
				chunk, err := s.liveChunk(ctx, hash)
				if err != nil {
					return nil, err
				}
				expected := int(min(int64(ChunkSize), node.Size-int64(i*ChunkSize)))
				if len(chunk) != expected {
					return nil, fmt.Errorf("invalid native WAL chunk size at %d", lsn)
				}
				payload = append(payload, chunk...)
			}
			if int64(len(payload)) != node.Size {
				return nil, fmt.Errorf("invalid native WAL size at %d", lsn)
			}
			batch.payloads = append(batch.payloads, payload)
		}
		// Warm newly referenced native data objects within an 8 MiB budget. Large
		// cold objects still use the normal native range-read path on demand.
		budget := int64(8 << 20)
		s.mu.Lock()
		hashes := []string{}
		selected := map[string]bool{}
		for k, n := range m.Files {
			if !strings.HasPrefix(k, "data:") || strings.HasPrefix(k, "data:__local__/") || strings.HasPrefix(k, "data:__live__/") {
				continue
			}
			if old, ok := s.files[k]; ok && slices.Equal(old.Chunks, n.Chunks) {
				continue
			}
			for _, hash := range n.Chunks {
				if selected[hash] || s.cache[hash] != nil || s.overlay[hash] != nil {
					continue
				}
				if budget < ChunkSize {
					break
				}
				hashes = append(hashes, hash)
				selected[hash] = true
				budget -= ChunkSize
			}
		}
		s.mu.Unlock()
		for _, hash := range hashes {
			if _, err := s.liveChunk(ctx, hash); err != nil {
				return nil, err
			}
		}
		s.livePrepareMu.Lock()
		s.livePrepared = append(s.livePrepared, batch)
		if len(s.livePrepared) > 2 {
			s.livePrepared = s.livePrepared[len(s.livePrepared)-2:]
		}
		s.livePrepareMu.Unlock()
		return batch, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.(*preparedLiveBatch), nil
	}
}

// LiveBatch imports only an already validated immutable batch. Engine admission
// remains serialized by the control endpoint and driver transaction boundary.
func (s *Store) LiveBatch(ctx context.Context, version string, after uint64) ([][]byte, uint64, error) {
	batch, err := s.prepareLiveBatch(ctx, version, after)
	if err != nil {
		return nil, after, err
	}
	// Keep a strong reference through admission even if background preparation
	// evicts this version from the two-entry cache concurrently.
	s.mu.Lock()
	for k, n := range batch.manifest.Files {
		if strings.HasPrefix(k, "data:") && !strings.HasPrefix(k, "data:__local__/") {
			s.files[k] = n
		}
	}
	s.mu.Unlock()
	return batch.payloads[after-batch.after:], batch.upto, nil
}

// The single-process experiment applies native table logtails directly to its
// own CN. This barrier completes before the driver admits another SQL query.
func (s *Store) RegisterLiveConsumer(fn func(context.Context, []logtail.TableLogtail) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveConsumer = fn
}
func (s *Store) ApplyLiveLogtails(ctx context.Context, tails []logtail.TableLogtail) error {
	s.mu.Lock()
	consumer := s.liveConsumer
	s.mu.Unlock()
	if consumer == nil {
		return fmt.Errorf("native query consumer not ready")
	}
	return consumer(ctx, tails)
}
