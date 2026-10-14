// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"time"
)

// ApplyBucketLogtails updates only tables already cached by this native CN.
// Unsubscribed tables get their complete state through native first subscribe.
// Source commit timestamps remain intact so SQL snapshots retain MVCC semantics.
func (e *Engine) ApplyBucketLogtails(ctx context.Context, tails []logtail.TableLogtail) error {
	applied := e.pClient.LatestLogtailAppliedTime()
	for _, tail := range tails {
		if !e.pClient.IsSubscribed(tail.Table.TbId) {
			continue
		}
		if err := e.consumeUpdateLogTail(ctx, tail, true, time.Now(), applied); err != nil {
			return err
		}
	}
	return nil
}
