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

package checkpointtool

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// VisibleSnapshotObjects composes repeated object lifecycle records across
// checkpoints before selecting visibility. Scanning raw checkpoint entries can
// double-count the same object or resurrect an object removed by compaction.
func VisibleSnapshotObjects(entries []*ObjectEntryInfo, ts types.TS) []*ObjectEntryInfo {
	return visibleObjectEntries(entries, ts)
}

// SnapshotColumns returns physical column types from the selected native objects.
// The caller must bind these to checkpoint catalog columns by sequence number.
func (r *CheckpointReader) SnapshotColumns(ctx context.Context, data *TableDumpData, ts types.TS) ([]objecttool.ColInfo, error) {
	return r.logicalTableColumns(ctx, visibleObjectEntries(data.DataEntries, ts))
}

// ReadSnapshotBlock returns an owned, typed batch in visible schema order, with
// commit timestamps and persisted tombstones applied at ts. It starts no service.
// This experimental adapter rejects missing columns/type changes rather than
// guessing schema-evolution defaults. The caller owns Clean(mp) on success.
func (r *CheckpointReader) ReadSnapshotBlock(ctx context.Context, data *TableDumpData, entry *ObjectEntryInfo, block uint16, ts types.TS, mp *mpool.MPool) (_ *batch.Batch, err error) {
	name := entry.ObjectStats.ObjectName().String()
	reader, err := objecttool.OpenWithFS(ctx, r.fs, name, name)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	physical, release, err := reader.ReadBlock(ctx, uint32(block))
	if err != nil {
		return nil, err
	}
	defer release()
	commits, releaseCommits, err := reader.ReadBlockCommitTS(ctx, uint32(block))
	if err != nil {
		return nil, err
	}
	if releaseCommits != nil {
		defer releaseCommits()
	}
	var commitTS []types.TS
	if commits != nil {
		commitTS = vector.MustFixedColWithTypeCheck[types.TS](commits)
	}
	tombs := dedupeObjectStats(visibleObjectEntries(data.TombEntries, ts))
	relevant, err := r.filterTombstonesForObject(ctx, entry.ObjectStats.ObjectName().ObjectId(), tombs)
	if err != nil {
		return nil, err
	}
	mask, err := r.buildDeleteMaskForBlock(ctx, &ts, entry.ObjectStats, block, relevant)
	if err != nil {
		return nil, err
	}
	if mask.IsValid() {
		defer mask.Release()
	}
	sels := make([]int64, 0, physical.RowCount())
	for row := 0; row < physical.RowCount(); row++ {
		if commits != nil && row < len(commitTS) && !commits.IsNull(uint64(row)) && commitTS[row].GT(&ts) {
			continue
		}
		if mask.IsValid() && mask.Contains(uint64(row)) {
			continue
		}
		sels = append(sels, int64(row))
	}
	bySeq := make(map[uint16]int)
	for i, col := range reader.Columns() {
		bySeq[col.SeqNum] = i
	}
	out := batch.NewWithSize(len(data.Schema.Columns))
	defer func() {
		if err != nil {
			out.Clean(mp)
		}
	}()
	for i, col := range data.Schema.Columns {
		pos, ok := bySeq[uint16(col.PhysicalPosition)]
		if !ok {
			return nil, fmt.Errorf("snapshot experiment: missing physical column %s (sequence %d)", col.Name, col.PhysicalPosition)
		}
		out.Vecs[i] = vector.NewVec(*physical.Vecs[pos].GetType())
		out.Attrs = append(out.Attrs, col.Name)
		if err = out.Vecs[i].Union(physical.Vecs[pos], sels, mp); err != nil {
			return nil, err
		}
	}
	out.SetRowCount(len(sels))
	return out, nil
}
