// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package db

import (
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	pblogtail "github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"net/http"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectstoragefs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

// One native TN exists in the experimental bucket runtime. The loopback
// control endpoint serializes source application; SQL admission is serialized
// by the local driver and paused while an explicit transaction is open.
func (db *DB) RegisterBucketLiveHTTP() {
	bucket := objectstoragefs.Current()
	if bucket == nil || !bucket.ReadOnly() || !bucket.LiveUpdates() {
		return
	}
	// Cold recovery initializes source object data before CN bootstrap writes.
	processor := new(catalog.LoopProcessor)
	processor.ObjectFn = func(entry *catalog.ObjectEntry) error {
		if data := entry.GetObjectData(); data != nil {
			data.FreezeAppend()
		}
		return nil
	}
	if err := db.Catalog.RecurLoop(processor); err != nil {
		panic(err)
	}
	var mu sync.Mutex
	var failed error
	applied := db.BucketReplayLSN
	http.HandleFunc("/_bucket/prefetch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		version := r.URL.Query().Get("version")
		if version == "" {
			http.Error(w, "version required", 400)
			return
		}
		mu.Lock()
		after, err := applied, failed
		mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		upto, err := bucket.PrepareLiveBatch(r.Context(), version, after)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": version, "lsn": upto})
	})
	http.HandleFunc("/_bucket/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if failed != nil {
			http.Error(w, failed.Error(), 503)
			return
		}
		version := r.URL.Query().Get("version")
		if version == "" {
			http.Error(w, "version required", 400)
			return
		}
		payloads, upto, err := bucket.LiveBatch(r.Context(), version, applied)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		cmds := make([]*txnbase.TxnCmd, 0, len(payloads))
		// Decode the entire batch before mutating native state.
		err = func() (err error) {
			defer func() {
				if v := recover(); v != nil {
					err = fmt.Errorf("native WAL decode: %v", v)
				}
			}()
			for i, p := range payloads {
				if len(p) < 4 {
					return fmt.Errorf("short native WAL")
				}
				h := objectio.DecodeIOEntryHeader(p)
				if h.Version < txnbase.IOET_WALTxnEntry_V4 {
					return fmt.Errorf("unsupported native WAL version")
				}
				decoded, e := objectio.GetIOEntryCodec(*h).Decode(p[4:])
				if e != nil {
					return e
				}
				cmd, ok := decoded.(*txnbase.TxnCmd)
				if !ok {
					return fmt.Errorf("invalid transaction command")
				}
				if len(cmd.Participants) > 1 {
					return fmt.Errorf("multiple TN participants unsupported")
				}
				cmd.Lsn = applied + uint64(i) + 1
				cmds = append(cmds, cmd)
			}
			return nil
		}()
		defer func() {
			for _, cmd := range cmds {
				cmd.Close()
			}
		}()
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		err = func() (err error) {
			defer func() {
				if v := recover(); v != nil {
					err = fmt.Errorf("native WAL apply failed: %v", v)
				}
			}()
			replayer := newWalReplayer(db, types.TS{}, applied)
			for _, cmd := range cmds {
				replayer.OnReplayTxn(cmd, cmd.Lsn)
			}

			if len(cmds) > 0 {
				if err := replayer.postReplayWal(); err != nil {
					return err
				}
				tables := map[[2]uint64]bool{}
				from, to := cmds[0].PrepareTS, cmds[len(cmds)-1].PrepareTS
				for _, cmd := range cmds {
					for _, record := range cmd.GetMemo().GetDirty().Tables {
						tables[[2]uint64{record.DbID, record.ID}] = true
					}
				}
				ids := make([][2]uint64, 0, len(tables))
				for id := range tables {
					ids = append(ids, id)
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i][0] < ids[j][0] || (ids[i][0] == ids[j][0] && ids[i][1] < ids[j][1]) })
				tails := make([]pblogtail.TableLogtail, 0, len(ids))
				closers := []func(){}
				defer func() {
					for _, close := range closers {
						if close != nil {
							close()
						}
					}
				}()
				for _, id := range ids {
					table := &api.TableID{DbId: id[0], TbId: id[1]}
					have, want := from.ToTimestamp(), to.ToTimestamp()
					resp, close, err := taelogtail.HandleSyncLogTailReq(r.Context(), db, db.LogtailMgr, db.Catalog, api.SyncLogTailReq{CnHave: &have, CnWant: &want, Table: table}, false)
					closers = append(closers, close)
					if err != nil {
						return err
					}
					commands := make([]api.Entry, len(resp.Commands))
					for i, entry := range resp.Commands {
						commands[i] = *entry
					}
					tails = append(tails, pblogtail.TableLogtail{Table: table, Ts: &want, Commands: commands, CkpLocation: resp.CkpLocation})
				}
				return bucket.ApplyLiveLogtails(r.Context(), tails)
			}
			return nil
		}()
		if err != nil {
			failed = err
			http.Error(w, err.Error(), 503)
			return
		}
		applied = upto
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": version, "lsn": applied, "entries": len(cmds)})
	})
}

// Live readers follow the source object's lifecycle. Private compaction or MVCC
// garbage collection would fork that lifecycle and discard history still needed
// by delayed source commands. Heartbeats and native query service remain active.
func (db *DB) isBucketLiveReader() bool {
	bucket := objectstoragefs.Current()
	return bucket != nil && bucket.ReadOnly() && bucket.LiveUpdates()
}
