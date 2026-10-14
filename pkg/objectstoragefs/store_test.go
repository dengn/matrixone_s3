package objectstoragefs

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	endpoint := os.Getenv("MO_BUCKET_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set MO_BUCKET_TEST_ENDPOINT for real S3 API integration")
	}
	return Config{Bucket: "mo-full-experiment", Prefix: "tests/" + uuid.NewString(), Endpoint: endpoint, Region: "us-east-1", AccessKey: "experiment", SecretKey: "experiment", Mode: "writer", LeaseSeconds: 60}
}

func TestBucketDurabilitySnapshotsAndExclusiveWriter(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	w, e := Open(ctx, cfg)
	require.NoError(t, e)
	_, e = Open(ctx, cfg)
	require.ErrorIs(t, e, ErrWriterBusy)
	require.NoError(t, w.MkdirAll("wal", 0755))
	f, e := w.Create("wal/commit")
	require.NoError(t, e)
	_, e = f.Write([]byte("durable commit"))
	require.NoError(t, e)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	require.NoError(t, w.WriteObject(ctx, "orders", []byte("version 1")))
	require.NoError(t, w.PublishVisible(ctx))
	rcfg := cfg
	rcfg.Mode = "reader"
	r, e := Open(ctx, rcfg)
	require.NoError(t, e)
	defer r.Close()
	oldVersion := r.Version()
	got, e := r.ReadObject(ctx, "orders", 0, -1)
	require.NoError(t, e)
	require.Equal(t, "version 1", string(got))
	require.NoError(t, w.DeleteObjects(ctx, []string{"orders"}))
	require.NoError(t, w.WriteObject(ctx, "orders", []byte("version 2")))
	rBefore, e := Open(ctx, rcfg)
	require.NoError(t, e)
	defer rBefore.Close()
	require.Equal(t, oldVersion, rBefore.Version())
	require.NoError(t, w.PublishVisible(ctx))
	rNew, e := Open(ctx, rcfg)
	require.NoError(t, e)
	defer rNew.Close()
	got, e = rNew.ReadObject(ctx, "orders", 0, -1)
	require.NoError(t, e)
	require.Equal(t, "version 2", string(got))
	got, e = r.ReadObject(ctx, "orders", 0, -1)
	require.NoError(t, e)
	require.Equal(t, "version 1", string(got))
	require.NoError(t, w.Close())
	w2, e := Open(ctx, cfg)
	require.NoError(t, e)
	defer w2.Close()
	restored, e := w2.Open("wal/commit")
	require.NoError(t, e)
	got, e = io.ReadAll(restored)
	require.NoError(t, e)
	require.Equal(t, "durable commit", string(got))
	require.NoError(t, restored.Close())
}

func TestExpiredWriterCannotPublishAfterTakeover(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	old, e := Open(ctx, cfg)
	require.NoError(t, e)
	// Freeze renewal and expire the lease to simulate a paused/dead process.
	close(old.stop)
	<-old.done
	old.mu.Lock()
	old.head.LeaseUntil = time.Now().Add(-time.Second).UnixMilli()
	e = old.saveHead(ctx)
	old.mu.Unlock()
	require.NoError(t, e)
	newWriter, e := Open(ctx, cfg)
	require.NoError(t, e)
	defer newWriter.Close()
	require.NoError(t, newWriter.WriteObject(ctx, "new", []byte("new owner")))
	e = old.WriteObject(ctx, "stale", []byte("must not be visible"))
	require.ErrorIs(t, e, ErrFenced)
	rcfg := cfg
	rcfg.Mode = "reader"
	reader, e := Open(ctx, rcfg)
	require.NoError(t, e)
	defer reader.Close()
	_, e = reader.ReadObject(ctx, "stale", 0, -1)
	require.ErrorIs(t, e, os.ErrNotExist)
	old.mu.Lock()
	old.closed = true
	old.mu.Unlock()
}

// An unrelated fsync or native-object write must never publish a partially
// written WAL file. Hard links must retain the same inode after cold recovery.
func TestOnlySyncedWALBytesArePublished(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, w.MkdirAll("wal", 0755))
	f, err := w.Create("wal/log")
	require.NoError(t, err)
	_, err = f.Write([]byte("committed"))
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, w.Link("wal/log", "wal/alias"))
	_, err = f.WriteAt([]byte("unfinished"), 0)
	require.NoError(t, err)
	require.NoError(t, w.WriteObject(ctx, "unrelated", []byte("object")))
	require.NoError(t, w.PublishVisible(ctx))
	rcfg := cfg
	rcfg.Mode = "reader"
	r, err := Open(ctx, rcfg)
	require.NoError(t, err)
	restored, err := r.Open("wal/log")
	require.NoError(t, err)
	b, err := io.ReadAll(restored)
	require.NoError(t, err)
	require.Equal(t, "committed", string(b))
	require.NoError(t, restored.Close())
	require.NoError(t, r.Close())
	require.NoError(t, w.Rename("wal/log", "wal/moved"))
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	require.NoError(t, w.Close())
	w, err = Open(ctx, cfg)
	require.NoError(t, err)
	defer w.Close()
	f, err = w.OpenForAppend("wal/alias")
	require.NoError(t, err)
	_, err = f.Write([]byte("!"))
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	restored, err = w.Open("wal/moved")
	require.NoError(t, err)
	b, err = io.ReadAll(restored)
	require.NoError(t, err)
	require.Equal(t, "unfinished!", string(b))
	require.NoError(t, restored.Close())
}

func TestReaderMaintenanceStaysInMemory(t *testing.T) {
	ctx := t.Context()
	cfg := testConfig(t)
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, w.WriteObject(ctx, "committed", []byte("original")))
	require.NoError(t, w.PublishVisible(ctx))
	require.NoError(t, w.Close())
	cfg.Mode = "reader"
	r, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer r.Close()
	before, etag, err := r.get(ctx, "head.json")
	require.NoError(t, err)
	require.NoError(t, r.ReplaceObject(ctx, "committed", []byte("local overlay")))
	require.NoError(t, r.MkdirAll("reader-only", 0755))
	f, err := r.Create("reader-only/log")
	require.NoError(t, err)
	_, err = f.Write([]byte("local WAL"))
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	require.NoError(t, r.PublishVisible(ctx))
	after, afterETag, err := r.get(ctx, "head.json")
	require.NoError(t, err)
	require.Equal(t, etag, afterETag)
	require.Equal(t, before, after)
	fresh, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer fresh.Close()
	data, err := fresh.ReadObject(ctx, "committed", 0, -1)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
	_, err = fresh.Open("reader-only/log")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestUnchangedSyncReusesManifestAndStillFences(t *testing.T) {
	ctx := t.Context()
	cfg := testConfig(t)
	w, err := Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, w.WriteObject(ctx, "stable", []byte("unchanged")))
	require.NoError(t, w.PublishVisible(ctx))
	version := w.Version()
	for i := 0; i < 10; i++ {
		require.NoError(t, w.PublishVisible(ctx))
		require.Equal(t, version, w.Version(), "no-change sync must reuse its immutable generation")
	}
	require.NoError(t, w.Close())
	w, err = Open(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, w.PublishVisible(ctx))
	require.Equal(t, version, w.Version(), "cold recovery must also reuse the unchanged manifest")
	close(w.stop)
	<-w.done
	w.mu.Lock()
	w.head.LeaseUntil = time.Now().Add(-time.Second).UnixMilli()
	err = w.saveHead(ctx)
	w.mu.Unlock()
	require.NoError(t, err)
	next, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer next.Close()
	require.ErrorIs(t, w.PublishVisible(ctx), ErrFenced, "no-change publication must not bypass the ownership CAS")
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}
