package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBucketReaderStatementPolicy(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		allowed bool
	}{
		{"select 1", true}, {"explain select 1", true}, {"explain analyze select 1", true}, {"explain analyze delete from t", false}, {"select * from t for update", false},
		{"insert into t values(1)", false}, {"delete from t", false},
		{"create table t(a int)", false}, {"begin", true}, {"commit", true},
		{"show tables", true}, {"use demo", true}, {"set @x=1", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			if tc.allowed {
				require.NoError(t, checkBucketReaderStatement(stmt))
			} else {
				require.Error(t, checkBucketReaderStatement(stmt))
			}
		})
	}
}

func TestBucketGenerationPublicationPolicy(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		publish bool
	}{
		{"select 1", false}, {"call p()", true}, {"drop procedure p", true}, {"explain analyze delete from t", true}, {"explain select 1", false}, {"prepare ps from @q", false}, {"insert into t values(1)", true},
		{"create table t(a int)", true}, {"begin", false}, {"commit", true},
		{"rollback", false}, {"show tables", false}, {"use demo", false},
		{"set autocommit=1", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			exec := &ExecCtx{stmt: stmt}
			require.Equal(t, tc.publish, bucketPublishesGeneration(exec))
			exec.input = &UserInput{isInternalInput: true}
			require.False(t, bucketPublishesGeneration(exec))
		})
	}
}
