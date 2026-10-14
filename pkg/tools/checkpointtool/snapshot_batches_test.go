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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSnapshotBlockOwnsTypedColumnsInCatalogOrder(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "typed.obj")
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	reader := &CheckpointReader{ctx: ctx, fs: fs, mp: mp}
	entry := &ObjectEntryInfo{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}
	data := &TableDumpData{DataEntries: []*ObjectEntryInfo{entry}, Schema: &TableSchema{Columns: []TableColumn{{Name: "name", PhysicalPosition: 1}, {Name: "id", PhysicalPosition: 0}}}}
	bat, err := reader.ReadSnapshotBlock(ctx, data, entry, 0, types.BuildTS(200, 0), mp)
	require.NoError(t, err)
	// ObjectReader and its cached vectors have already been released here.
	require.Equal(t, []string{"name", "id"}, bat.Attrs)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, "alice", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[1], 1))
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestSnapshotBlockMissingSchemaColumnFailsWithoutLeaking(t *testing.T) {
	ctx := context.Background()
	fs, stats := writeLogicalTableTestObject(t, "missing.obj")
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	reader := &CheckpointReader{ctx: ctx, fs: fs, mp: mp}
	entry := &ObjectEntryInfo{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}
	data := &TableDumpData{Schema: &TableSchema{Columns: []TableColumn{{Name: "id", PhysicalPosition: 0}, {Name: "missing", PhysicalPosition: 99}}}}
	bat, err := reader.ReadSnapshotBlock(ctx, data, entry, 0, types.BuildTS(200, 0), mp)
	require.ErrorContains(t, err, "missing physical column")
	require.Nil(t, bat)
	require.Zero(t, mp.CurrNB())
}
