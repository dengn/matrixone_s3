// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package objectstoragefs

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
)

func TestLiveBatchPreservesPrivateControlAndValidatesGaps(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	cfg.LiveUpdates = true
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, w.ExportLiveWAL(ctx, 1, []byte("first native command")))
	require.NoError(t, w.WriteObject(ctx, "__local__/control", []byte("writer")))
	require.NoError(t, w.WriteObject(ctx, "user-object", []byte("immutable native object")))
	require.NoError(t, w.PublishVisible(ctx))
	cfg.Mode = "reader"
	r, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer r.Close()
	require.NoError(t, r.ReplaceObject(ctx, "__local__/control", []byte("private reader")))
	require.NoError(t, w.ExportLiveWAL(ctx, 2, []byte("second native command")))
	require.NoError(t, w.PublishVisible(ctx))
	payloads, upto, err := r.LiveBatch(ctx, w.Version(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), upto)
	require.Equal(t, [][]byte{[]byte("second native command")}, payloads)
	control, err := r.ReadObject(ctx, "__local__/control", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "private reader", string(control))
	// A missing journal entry cannot silently skip to a newer source frontier.
	require.NoError(t, w.ExportLiveWAL(ctx, 4, []byte("after a gap")))
	require.NoError(t, w.PublishVisible(ctx))
	_, _, err = r.LiveBatch(ctx, w.Version(), 2)
	require.ErrorContains(t, err, "missing live WAL 3")
}

func TestLivePrefetchKeepsVisibilityAndCachesAdmission(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	cfg.LiveUpdates = true
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, w.ExportLiveWAL(ctx, 1, []byte("baseline")))
	require.NoError(t, w.PublishVisible(ctx))
	cfg.Mode = "reader"
	r, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer r.Close()
	old := r.Version()
	require.NoError(t, w.WriteObject(ctx, "new-native-object", []byte("new immutable object")))
	require.NoError(t, w.ExportLiveWAL(ctx, 2, []byte("new native command")))
	require.NoError(t, w.PublishVisible(ctx))
	version := w.Version()
	// Concurrent background/query requests share a prepared immutable frontier.
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := r.PrepareLiveBatch(ctx, version, 1); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, old, r.Version())
	_, err = r.ObjectSize("new-native-object")
	require.ErrorIs(t, err, os.ErrNotExist)
	// Make remote reads impossible: admission and the newly warmed native object
	// must be available entirely from the validated prepared batch/cache.
	r.client = s3.New(s3.Options{Region: cfg.Region, BaseEndpoint: aws.String("http://127.0.0.1:1"), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")})
	payloads, upto, err := r.LiveBatch(ctx, version, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), upto)
	require.Equal(t, [][]byte{[]byte("new native command")}, payloads)
	data, err := r.ReadObject(ctx, "new-native-object", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "new immutable object", string(data))
	payloads, upto, err = r.LiveBatch(ctx, version, 2)
	require.NoError(t, err)
	require.Empty(t, payloads)
	require.Equal(t, uint64(2), upto)
}

func TestLivePrefetchRejectsCorruptionBeforeImport(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	cfg.LiveUpdates = true
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer w.Close()
	require.NoError(t, w.ExportLiveWAL(ctx, 1, []byte("baseline")))
	require.NoError(t, w.PublishVisible(ctx))
	cfg.Mode = "reader"
	r, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer r.Close()
	require.NoError(t, w.WriteObject(ctx, "not-yet-visible", []byte("new data")))
	require.NoError(t, w.ExportLiveWAL(ctx, 2, []byte("command to corrupt")))
	require.NoError(t, w.PublishVisible(ctx))
	hash := w.files[liveName(2)].Chunks[0]
	require.NoError(t, w.put(ctx, "chunks/"+hash, []byte("damaged command")))
	_, err = r.PrepareLiveBatch(ctx, w.Version(), 1)
	require.ErrorContains(t, err, "checksum")
	require.Nil(t, r.cachedLiveBatch(w.Version(), 1))
	_, err = r.ObjectSize("not-yet-visible")
	require.ErrorIs(t, err, os.ErrNotExist)
}
