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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCatalogDecimalTypeRetainsSemanticScale(t *testing.T) {
	typ, err := catalogPlanType(context.Background(), "DECIMAL(12,2)")
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal64), typ.Id)
	require.Equal(t, int32(12), typ.Width)
	require.Equal(t, int32(2), typ.Scale)
}

func TestAggregateDecimalUsesOutputSchemaScale(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	// Native aggregate vectors can retain a physical type with zero scale.
	vec := vector.NewVec(types.T_decimal256.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixed(vec, types.Decimal256{B0_63: 1234}, false, mp))
	value, err := jsonValue(vec, 0, pb.Type{Id: int32(types.T_decimal256), Scale: 2})
	require.NoError(t, err)
	require.Equal(t, "12.34", value)
}
