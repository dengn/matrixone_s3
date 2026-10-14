// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// mo-bucket-query is an experimental, service-free native checkpoint reader.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/toolfs"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type result struct {
	Snapshot     string   `json:"snapshot"`
	Columns      []string `json:"columns"`
	Rows         [][]any  `json:"rows"`
	ElapsedMS    float64  `json:"elapsed_ms"`
	NativeBlocks int      `json:"native_blocks_read"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("directory", "", "local native data directory (offline only)")
	kind := flag.String("kind", "local2", "local object framing: local or local2")
	backend := flag.String("backend", "S3", "S3 or MINIO")
	remote := flag.String("remote-s3", "", "bucket/endpoint/key-prefix options; use MO_BUCKET_S3 to keep credentials out of argv")
	tsText := flag.String("snapshot", "", "physical:logical timestamp; default latest checkpoint end")
	database := flag.String("database", "bucket_demo", "default database")
	sql := flag.String("sql", "", "one read-only SELECT")
	inspect := flag.Bool("inspect", false, "print latest checkpoint timestamp and catalog")
	showPlan := flag.Bool("plan", false, "print native MO logical plan on stderr")
	flag.Parse()
	logutil.SetupMOLogger(&logutil.LogConfig{Level: "error"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))
	var fs fileservice.FileService
	var reader *checkpointtool.CheckpointReader
	var err error
	if *remote == "" {
		*remote = os.Getenv("MO_BUCKET_S3")
	}
	if *remote != "" {
		args, e := toolfs.ParseS3Arguments(*remote, "SHARED")
		if e != nil {
			return e
		}
		fs, err = fileservice.NewFileService(ctx, fileservice.Config{Name: "SHARED", Backend: *backend, S3: args, Cache: fileservice.DisabledCacheConfig}, nil)
		if err != nil {
			return err
		}
		defer fs.Close(ctx)
		if err := requirePublishedSnapshot(ctx, fs); err != nil {
			return err
		}
		reader, err = checkpointtool.OpenWithFS(ctx, fs, "")
	} else {
		if *dir == "" {
			return fmt.Errorf("provide --remote-s3 / MO_BUCKET_S3 or --directory")
		}
		reader, err = checkpointtool.Open(ctx, *dir, checkpointtool.WithKind(*kind))
	}
	if err != nil {
		return err
	}
	defer reader.Close()
	var ts types.TS
	for _, entry := range reader.Entries() {
		end := entry.GetEnd()
		if ts.LT(&end) {
			ts = end
		}
	}
	latest := ts
	if *tsText != "" {
		parts := strings.Split(*tsText, ":")
		if len(parts) != 2 {
			return fmt.Errorf("snapshot must be physical:logical")
		}
		physical, e := strconv.ParseInt(parts[0], 10, 64)
		if e != nil {
			return e
		}
		logical, e := strconv.ParseUint(parts[1], 10, 32)
		if e != nil {
			return e
		}
		ts = types.BuildTS(physical, uint32(logical))
	}
	if ts.GT(&latest) {
		return fmt.Errorf("snapshot is newer than the latest published checkpoint")
	}
	if ts.IsEmpty() {
		return fmt.Errorf("no published checkpoint")
	}
	if err := reader.ValidateSnapshot(ctx, ts); err != nil {
		return err
	}
	account := uint32(0)
	tables, err := reader.ListCatalogTables(ctx, ts, checkpointtool.TableListOptions{AccountID: &account})
	if err != nil {
		return err
	}
	if *inspect {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"snapshot": fmt.Sprintf("%d:%d", ts.Physical(), ts.Logical()), "tables": tables})
	}
	if *sql == "" {
		return fmt.Errorf("provide --sql or --inspect")
	}
	start := time.Now()
	mp, err := mpool.NewMPool("bucket-query", 128*mpool.MB, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)
	proc := process.NewTopProcess(ctx, mp, nil, nil, reader.FS(), nil, nil, nil, nil, nil, nil)
	defer proc.Free()
	proc.Base.SessionInfo.TimeZone = time.UTC
	proc.SetResolveVariableFunc(resolveVariable)
	cc := &snapshotContext{ctx: ctx, proc: proc, reader: reader, ts: ts, database: *database, sql: *sql, catalog: tables, tables: make(map[uint64]*boundTable), stats: plan.NewStatsCache()}
	proc.Base.SessionInfo.CompilerContext = cc
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, *sql, 1)
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return fmt.Errorf("exactly one SELECT is supported")
	}
	selectStmt, ok := stmts[0].(*tree.Select)
	if !ok {
		return fmt.Errorf("read-only snapshot: only SELECT is supported")
	}
	if tree.SelectIntoExport(selectStmt.Select) != nil || selectStmt.Ep != nil || len(selectStmt.IntoVars) > 0 || selectStmt.SelectLockInfo != nil {
		return fmt.Errorf("read-only snapshot: INTO/locking clauses are unsupported")
	}
	query, err := plan.NewBaseOptimizer(cc).Optimize(stmts[0], false)
	if err != nil {
		return err
	}
	if *showPlan {
		data, _ := json.MarshalIndent(query, "", "  ")
		fmt.Fprintln(os.Stderr, string(data))
	}
	if len(query.Steps) != 1 {
		return fmt.Errorf("experiment supports one query step")
	}
	builder := &pipelineBuilder{cc: cc, query: query}
	defer builder.close(proc)
	root, err := builder.build(query.Steps[0])
	if err != nil {
		return err
	}
	if err := vm.Prepare(root, proc); err != nil {
		return err
	}
	res := result{Snapshot: fmt.Sprintf("%d:%d", ts.Physical(), ts.Logical()), Columns: query.Headings, Rows: make([][]any, 0)}
	// Keep the standalone experiment bounded; production should stream results.
	const maxResultRows = 100000
	resultBytes := 0
	outputExprs := query.Nodes[query.Steps[0]].ProjectList
	for {
		call, e := vm.Exec(root, proc)
		if e != nil {
			return e
		}
		if call.Batch != nil {
			for row := 0; row < call.Batch.RowCount(); row++ {
				if len(res.Rows) >= maxResultRows {
					return fmt.Errorf("experiment result exceeds %d rows; add LIMIT", maxResultRows)
				}
				values := make([]any, len(call.Batch.Vecs))
				for col, v := range call.Batch.Vecs {
					if col >= len(outputExprs) {
						return fmt.Errorf("output schema mismatch")
					}
					value, e := jsonValue(v, row, outputExprs[col].Typ)
					if e != nil {
						return e
					}
					values[col] = value
				}
				encoded, e := json.Marshal(values)
				if e != nil {
					return e
				}
				resultBytes += len(encoded)
				if resultBytes > 64<<20 {
					return fmt.Errorf("experiment result exceeds 64 MiB; add LIMIT")
				}
				res.Rows = append(res.Rows, values)
			}
		}
		if call.Status == vm.ExecStop || call.Batch == nil {
			break
		}
	}
	res.NativeBlocks = builder.blocks
	res.ElapsedMS = float64(time.Since(start).Microseconds()) / 1000
	return json.NewEncoder(os.Stdout).Encode(res)
}

func requirePublishedSnapshot(ctx context.Context, fs fileservice.FileService) error {
	vector := fileservice.IOVector{FilePath: "snapshot.json", Entries: []fileservice.IOEntry{{Offset: 0, Size: -1}}}
	if err := fs.Read(ctx, &vector); err != nil {
		return fmt.Errorf("snapshot is not published (snapshot.json must be written last): %w", err)
	}
	defer vector.Release()
	var marker struct {
		Format  string            `json:"format"`
		Count   int               `json:"count"`
		Objects []json.RawMessage `json:"objects"`
	}
	if err := json.Unmarshal(vector.Entries[0].Data, &marker); err != nil {
		return fmt.Errorf("invalid snapshot marker: %w", err)
	}
	if marker.Format != "matrixone-native-checkpoint-experiment-v1" || marker.Count == 0 || marker.Count != len(marker.Objects) {
		return fmt.Errorf("invalid snapshot marker format or inventory")
	}
	return nil
}

// boundTable keeps schema and object lists pinned to the one selected timestamp.
type boundTable struct {
	data *checkpointtool.TableDumpData
	def  *pb.TableDef
	ref  *pb.ObjectRef
}

// snapshotSource lazily reads one native block at a time. It holds no transaction,
// logtail client, TN, HAKeeper or RPC connection.
type snapshotSource struct {
	vm.OperatorBase
	builder *pipelineBuilder
	table   *boundTable
	cols    []*pb.ColDef
	object  int
	block   uint16
	owned   *batch.Batch
}
