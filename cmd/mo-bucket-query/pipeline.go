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
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type pipelineBuilder struct {
	cc        *snapshotContext
	query     *pb.Query
	operators []vm.Operator
	blocks    int
}

func (b *pipelineBuilder) add(op vm.Operator, child vm.Operator) vm.Operator {
	op.GetOperatorBase().SetIdx(len(b.operators))
	if child != nil {
		op.AppendChild(child)
	}
	b.operators = append(b.operators, op)
	return op
}
func (b *pipelineBuilder) close(proc *process.Process) {
	for i := len(b.operators) - 1; i >= 0; i-- {
		op := b.operators[i]
		op.Free(proc, false, nil)
		op.Release()
	}
}

// This deliberately small local compiler reuses production MO operators. It
// rejects every unsupported logical node instead of silently approximating SQL.
func (b *pipelineBuilder) build(id int32) (vm.Operator, error) {
	n := b.query.Nodes[id]
	var root vm.Operator
	var err error
	if len(n.Children) > 1 {
		return nil, fmt.Errorf("experiment does not implement multi-input node %s (joins/unions)", n.NodeType)
	}
	if len(n.Children) == 1 {
		root, err = b.build(n.Children[0])
		if err != nil {
			return nil, err
		}
	}
	switch n.NodeType {
	case pb.Node_TABLE_SCAN:
		table := b.cc.tables[n.TableDef.TblId]
		if table == nil {
			return nil, fmt.Errorf("unbound table %d", n.TableDef.TblId)
		}
		root = b.add(&snapshotSource{builder: b, table: table, cols: n.TableDef.Cols}, nil)
	case pb.Node_FILTER:
	case pb.Node_AGG:
		arg := group.NewArgument()
		arg.NeedEval = true
		arg.GroupBy = n.GroupBy
		arg.GroupingFlag = n.GroupingFlag
		arg.GroupByHashKey = n.GroupByHashKey
		arg.SpillMem = 64 << 20
		for _, expr := range n.AggList {
			f := expr.GetF()
			if f == nil {
				arg.Release()
				return nil, fmt.Errorf("invalid aggregate")
			}
			switch f.Func.ObjName {
			case "count", "starcount", "sum", "min", "max", "avg":
			default:
				arg.Release()
				return nil, fmt.Errorf("unsupported experimental aggregate %s", f.Func.ObjName)
			}
			fid := int64(uint64(f.Func.Obj) & function.DistinctMask)
			distinct := uint64(f.Func.Obj)&function.Distinct != 0
			arg.Aggs = append(arg.Aggs, aggexec.MakeAggFunctionExpression(fid, distinct, f.Args, nil, f.AggConfigType))
		}
		root = b.add(arg, root)
	case pb.Node_PROJECT:
	case pb.Node_SORT:
		arg := order.NewArgument()
		arg.OrderBySpec = n.OrderBy
		root = b.add(arg, root)
	default:
		return nil, fmt.Errorf("unsupported experiment plan node: %s", n.NodeType)
	}
	if len(n.FilterList) > 0 {
		arg := filter.NewArgument()
		arg.FilterExprs = n.FilterList
		root = b.add(arg, root)
	}
	if len(n.ProjectList) > 0 {
		arg := projection.NewArgument()
		arg.ProjectList = n.ProjectList
		root = b.add(arg, root)
	}
	if n.Offset != nil {
		arg := offset.NewArgument()
		arg.OffsetExpr = n.Offset
		root = b.add(arg, root)
	}
	if n.Limit != nil {
		arg := limit.NewArgument()
		arg.LimitExpr = n.Limit
		root = b.add(arg, root)
	}
	return root, nil
}

func (s *snapshotSource) GetOperatorBase() *vm.OperatorBase { return &s.OperatorBase }
func (s *snapshotSource) OpType() vm.OpType                 { return vm.TableScan }
func (s *snapshotSource) String(b *bytes.Buffer)            { b.WriteString("native_checkpoint_scan") }
func (s *snapshotSource) Prepare(proc *process.Process) error {
	s.OpAnalyzer = process.NewAnalyzer(s.GetIdx(), true, false, "native_checkpoint_scan")
	return nil
}
func (s *snapshotSource) ExecProjection(_ *process.Process, in *batch.Batch) (*batch.Batch, error) {
	return in, nil
}
func (s *snapshotSource) Reset(proc *process.Process, _ bool, _ error) {
	s.Free(proc, false, nil)
	s.object = 0
	s.block = 0
}
func (s *snapshotSource) Free(proc *process.Process, _ bool, _ error) {
	if s.owned != nil {
		s.owned.Clean(proc.Mp())
		s.owned = nil
	}
}
func (s *snapshotSource) Release() {}
func (s *snapshotSource) Call(proc *process.Process) (vm.CallResult, error) {
	s.Free(proc, false, nil)
	for s.object < len(s.table.data.DataEntries) {
		entry := s.table.data.DataEntries[s.object]
		ts := s.builder.cc.ts
		if entry.CreateTime.GT(&ts) || (!entry.DeleteTime.IsEmpty() && entry.DeleteTime.LE(&ts)) || uint32(s.block) >= entry.ObjectStats.BlkCnt() {
			s.object++
			s.block = 0
			continue
		}
		bat, err := s.builder.cc.reader.ReadSnapshotBlock(proc.Ctx, s.table.data, entry, s.block, ts, proc.Mp())
		if err != nil {
			return vm.CallResult{}, err
		}
		s.block++
		s.builder.blocks++
		if bat.RowCount() == 0 {
			bat.Clean(proc.Mp())
			continue
		}
		// Optimized scans prune/reorder columns. Transfer ownership of requested
		// vectors and free the unused ones before downstream execution.
		out := batch.NewWithSize(len(s.cols))
		out.SetRowCount(bat.RowCount())
		for i, col := range s.cols {
			pos := -1
			for j, original := range s.table.data.Schema.Columns {
				if original.Name == col.Name {
					pos = j
					break
				}
			}
			if pos < 0 {
				out.Clean(proc.Mp())
				bat.Clean(proc.Mp())
				return vm.CallResult{}, fmt.Errorf("unsupported scan column %s", col.Name)
			}
			if int32(bat.Vecs[pos].GetType().Oid) != col.Typ.Id {
				out.Clean(proc.Mp())
				bat.Clean(proc.Mp())
				return vm.CallResult{}, fmt.Errorf("schema type mismatch for %s", col.Name)
			}
			out.Vecs[i] = bat.Vecs[pos]
			out.Vecs[i].SetType(types.MustTypeFromPlan(col.Typ))
			bat.Vecs[pos] = nil
			out.Attrs = append(out.Attrs, col.Name)
		}
		bat.Clean(proc.Mp())
		s.owned = out
		return vm.CallResult{Batch: out, Status: vm.ExecNext}, nil
	}
	return vm.CallResult{Status: vm.ExecStop}, nil
}

func jsonValue(v *vector.Vector, row int, typ pb.Type) (any, error) {
	if v.IsNull(uint64(row)) {
		return nil, nil
	}
	i := row
	if v.IsConst() {
		i = 0
	}
	switch v.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](v, i), nil
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](v, i), nil
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](v, i), nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](v, i), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](v, i), nil
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](v, i), nil
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](v, i), nil
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](v, i), nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](v, i), nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](v, i), nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](v, i), nil
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](v, i).Format(typ.Scale), nil
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](v, i).Format(typ.Scale), nil
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](v, i).Format(typ.Scale), nil
	case types.T_char, types.T_varchar, types.T_text:
		return v.GetStringAt(i), nil
	default:
		return nil, fmt.Errorf("unsupported experiment result type: %s", v.GetType().String())
	}
}
