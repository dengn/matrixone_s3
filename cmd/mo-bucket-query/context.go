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

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	stats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type snapshotContext struct {
	ctx           context.Context
	proc          *process.Process
	reader        *checkpointtool.CheckpointReader
	ts            types.TS
	database, sql string
	catalog       []checkpointtool.TableCatalogEntry
	tables        map[uint64]*boundTable
	stats         *plan.StatsCache
}

var _ plan.CompilerContext = (*snapshotContext)(nil)

func (c *snapshotContext) bind(entry checkpointtool.TableCatalogEntry) (*boundTable, error) {
	if table, ok := c.tables[entry.TableID]; ok {
		return table, nil
	}
	if entry.RelKind != "r" {
		return nil, fmt.Errorf("experiment supports ordinary tables only: %s.%s (%s)", entry.DatabaseName, entry.TableName, entry.RelKind)
	}
	data, err := c.reader.PrepareTableDumpData(c.ctx, entry.TableID, c.ts)
	if err != nil {
		return nil, err
	}
	data.DataEntries = checkpointtool.VisibleSnapshotObjects(data.DataEntries, c.ts)
	data.TombEntries = checkpointtool.VisibleSnapshotObjects(data.TombEntries, c.ts)
	columns, err := c.reader.SnapshotColumns(c.ctx, data, c.ts)
	if err != nil {
		return nil, err
	}
	physical := make(map[uint16]types.Type)
	for _, col := range columns {
		physical[col.SeqNum] = col.Type
	}
	def := &pb.TableDef{TblId: entry.TableID, Name: entry.TableName, DbName: entry.DatabaseName, TableType: "r", Name2ColIndex: make(map[string]int32)}
	for i, col := range data.Schema.Columns {
		typ, ok := physical[uint16(col.PhysicalPosition)]
		if !ok {
			return nil, fmt.Errorf("no native type for column %s; empty tables/schema evolution are not supported in this experiment", col.Name)
		}
		// Object headers carry physical OIDs, while width/scale live in the
		// catalog. Bind semantic types from mo_columns, never guess decimal scale.
		semantic, err := catalogPlanType(c.ctx, col.SQLType)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		if semantic.Id != int32(typ.Oid) {
			return nil, fmt.Errorf("catalog/object type mismatch for %s", col.Name)
		}
		semantic.NotNullable = col.NotNull
		def.Cols = append(def.Cols, &pb.ColDef{ColId: uint64(i + 1), Name: col.Name, OriginName: col.Name, Seqnum: uint32(col.PhysicalPosition), Typ: semantic})
		def.Name2ColIndex[col.Name] = int32(i)
	}
	ref := &pb.ObjectRef{Db: int64(entry.DatabaseID), Obj: int64(entry.TableID), SchemaName: entry.DatabaseName, ObjName: entry.TableName}
	table := &boundTable{data: data, def: def, ref: ref}
	c.tables[entry.TableID] = table
	return table, nil
}

func catalogPlanType(ctx context.Context, sqlType string) (pb.Type, error) {
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, "create table __snapshot_type (v "+sqlType+")", 1)
	if err != nil {
		return pb.Type{}, err
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return pb.Type{}, fmt.Errorf("invalid catalog type")
	}
	create, ok := stmts[0].(*tree.CreateTable)
	if !ok || len(create.Defs) != 1 {
		return pb.Type{}, fmt.Errorf("invalid catalog type")
	}
	column, ok := create.Defs[0].(*tree.ColumnTableDef)
	if !ok {
		return pb.Type{}, fmt.Errorf("invalid catalog type")
	}
	return plan.GetTypeFromAst(ctx, column.Type)
}

func (c *snapshotContext) Resolve(db, table string, s *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	if s != nil {
		return nil, nil, fmt.Errorf("use --snapshot to select the single query snapshot")
	}
	if db == "" {
		db = c.database
	}
	for _, entry := range c.catalog {
		if strings.EqualFold(db, entry.DatabaseName) && strings.EqualFold(table, entry.TableName) {
			t, err := c.bind(entry)
			if err != nil {
				return nil, nil, err
			}
			return t.ref, plan.DeepCopyTableDef(t.def, true), nil
		}
	}
	return nil, nil, fmt.Errorf("table %s.%s is absent at snapshot %s", db, table, c.ts.ToString())
}
func (c *snapshotContext) ResolveById(id uint64, s *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	for _, e := range c.catalog {
		if e.TableID == id {
			return c.Resolve(e.DatabaseName, e.TableName, s)
		}
	}
	return nil, nil, fmt.Errorf("table %d absent", id)
}
func (c *snapshotContext) ResolveIndexTableByRef(r *plan.ObjectRef, n string, s *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return nil, nil, fmt.Errorf("index scans are unsupported in the experiment")
}
func (c *snapshotContext) DefaultDatabase() string { return c.database }
func (c *snapshotContext) DatabaseExists(n string, s *plan.Snapshot) bool {
	for _, e := range c.catalog {
		if strings.EqualFold(e.DatabaseName, n) {
			return true
		}
	}
	return false
}
func (c *snapshotContext) GetDatabaseId(n string, s *plan.Snapshot) (uint64, error) {
	for _, e := range c.catalog {
		if strings.EqualFold(e.DatabaseName, n) {
			return e.DatabaseID, nil
		}
	}
	return 0, fmt.Errorf("database absent: %s", n)
}
func (c *snapshotContext) GetContext() context.Context    { return c.ctx }
func (c *snapshotContext) SetContext(ctx context.Context) { c.ctx = ctx }
func (c *snapshotContext) GetProcess() *process.Process   { return c.proc }
func (c *snapshotContext) GetRootSql() string             { return c.sql }
func (c *snapshotContext) GetUserName() string            { return "bucket-reader" }
func (c *snapshotContext) GetAccountId() (uint32, error)  { return 0, nil }
func (c *snapshotContext) GetAccountName() string         { return "sys" }
func (c *snapshotContext) GetLowerCaseTableNames() int64  { return 1 }
func (c *snapshotContext) Stats(*plan.ObjectRef, *plan.Snapshot) (*stats.StatsInfo, error) {
	return nil, nil
}
func (c *snapshotContext) GetStatsCache() *plan.StatsCache { return c.stats }
func (c *snapshotContext) ResolveVariable(n string, sys, global bool) (interface{}, error) {
	return resolveVariable(n, sys, global)
}
func resolveVariable(n string, sys, global bool) (interface{}, error) {
	switch strings.ToLower(n) {
	case "sql_mode":
		return "ONLY_FULL_GROUP_BY", nil
	case "lower_case_table_names", "foreign_key_checks", "div_precision_increment":
		return int64(1), nil
	case "sort_spill_mem", "group_by_spill_mem":
		return int64(64 << 20), nil
	case "group_concat_max_len":
		return uint64(1024), nil
	case "time_zone":
		return "+00:00", nil
	default:
		return nil, fmt.Errorf("unsupported variable: %s", n)
	}
}
func (c *snapshotContext) ResolveAccountIds([]string) ([]uint32, error) {
	return nil, fmt.Errorf("accounts unsupported")
}
func (c *snapshotContext) ResolveUdf(string, []*plan.Expr) (*function.Udf, error) {
	return nil, fmt.Errorf("UDF unsupported")
}
func (c *snapshotContext) GetQueryResultMeta(string) ([]*plan.ColDef, string, error) {
	return nil, "", fmt.Errorf("saved results unsupported")
}
func (c *snapshotContext) SetBuildingAlterView(bool, string, string)    {}
func (c *snapshotContext) GetBuildingAlterView() (bool, string, string) { return false, "", "" }
func (c *snapshotContext) GetSubscriptionMeta(string, *plan.Snapshot) (*plan.SubscriptionMeta, error) {
	return nil, nil
}
func (c *snapshotContext) CheckSubscriptionValid(string, string, string) error {
	return fmt.Errorf("subscriptions unsupported")
}
func (c *snapshotContext) SetQueryingSubscription(*plan.SubscriptionMeta)  {}
func (c *snapshotContext) GetQueryingSubscription() *plan.SubscriptionMeta { return nil }
func (c *snapshotContext) IsPublishing(string) (bool, error)               { return false, nil }
func (c *snapshotContext) BuildTableDefByMoColumns(db, table string) (*plan.TableDef, error) {
	_, d, e := c.Resolve(db, table, nil)
	return d, e
}
func (c *snapshotContext) ResolveSubscriptionTableById(uint64, *plan.SubscriptionMeta) (*plan.ObjectRef, *plan.TableDef, error) {
	return nil, nil, fmt.Errorf("subscriptions unsupported")
}
func (c *snapshotContext) ResolveSnapshotWithSnapshotName(string) (*plan.Snapshot, error) {
	return nil, fmt.Errorf("use --snapshot")
}
func (c *snapshotContext) CheckTimeStampValid(int64) (bool, error) {
	return false, fmt.Errorf("use --snapshot")
}
func (c *snapshotContext) InitExecuteStmtParam(*pb.Execute) (*pb.Plan, tree.Statement, error) {
	return nil, nil, fmt.Errorf("prepared statements unsupported")
}
func (c *snapshotContext) GetSnapshot() *plan.Snapshot { return nil }
func (c *snapshotContext) SetSnapshot(*plan.Snapshot)  {}
func (c *snapshotContext) GetViews() []string          { return nil }
func (c *snapshotContext) SetViews([]string)           {}
