package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

func checkBucketReaderStatement(stmt tree.Statement) error {
	switch explain := stmt.(type) {
	case *tree.ExplainStmt:
		return checkBucketReaderStatement(explain.Statement)
	case *tree.ExplainAnalyze:
		return checkBucketReaderStatement(explain.Statement)
	case *tree.ExplainPhyPlan:
		return checkBucketReaderStatement(explain.Statement)
	}

	if selectStmt, ok := stmt.(*tree.Select); ok {
		if selectStmt.Ep != nil || tree.SelectIntoExport(selectStmt.Select) != nil || selectStmt.SelectLockInfo != nil {
			return moerr.NewInvalidInputNoCtx("bucket reader is read-only")
		}
	}
	if stmt.GetQueryType() == tree.QueryTypeDQL || stmt.GetQueryType() == tree.QueryTypeTCL {
		return nil
	}
	switch stmt.(type) {
	case *tree.Use, *tree.SetVar, *tree.PrepareStmt, *tree.PrepareString, *tree.PrepareVar, *tree.Deallocate:
		return nil
	}
	if strings.HasPrefix(strings.ToLower(stmt.GetStatementType()), "show") {
		return nil
	}
	return moerr.NewInvalidInputNoCtx("bucket reader is read-only")
}

// Reader generations change only for successful user mutations or transaction
// boundaries. SELECT and internal maintenance must not churn visible snapshots.
func bucketPublishesGeneration(execCtx *ExecCtx) bool {
	if execCtx.input != nil && execCtx.input.isInternal() {
		return false
	}
	stmt := execCtx.effectiveTxnStatement
	if stmt == nil {
		stmt = execCtx.stmt
	}
	if stmt == nil {
		return false
	}
	return bucketStatementPublishes(stmt)
}

func bucketStatementPublishes(stmt tree.Statement) bool {
	if stmt == nil {
		return false
	}
	switch explain := stmt.(type) {
	case *tree.ExplainStmt:
		return bucketStatementPublishes(explain.Statement)
	case *tree.ExplainAnalyze:
		return bucketStatementPublishes(explain.Statement)
	case *tree.ExplainPhyPlan:
		return bucketStatementPublishes(explain.Statement)
	}
	if stmt.GetQueryType() == tree.QueryTypeDQL {
		return false
	}
	if stmt.GetQueryType() == tree.QueryTypeTCL {
		_, commit := stmt.(*tree.CommitTransaction)
		return commit
	}
	switch stmt.(type) {
	case *tree.Use, *tree.PrepareStmt, *tree.PrepareString, *tree.PrepareVar, *tree.Deallocate:
		return false
	}
	if strings.HasPrefix(strings.ToLower(stmt.GetStatementType()), "show") {
		return false
	}
	// CALL, CREATE PROCEDURE and DROP PROCEDURE are classified as "Other" by
	// the native parser. Conservatively publish successful unclassified user
	// statements as well; their writes must not wait for a subsequent DML.
	return true
}
