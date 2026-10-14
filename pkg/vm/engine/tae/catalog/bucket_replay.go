// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/objectstoragefs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

// BucketReplayObjectFactory reserves the source object's row-offset namespace
// for native source WAL. Reader-local control transactions allocate another
// appendable object instead of interleaving rows into this imported object.
// FreezeAppend rejects local appenders; native replay still applies payloads.
func BucketReplayObjectFactory(factory ObjectDataFactory) ObjectDataFactory {
	bucket := objectstoragefs.Current()
	if factory == nil || bucket == nil || !bucket.ReadOnly() || !bucket.LiveUpdates() {
		return factory
	}
	return func(meta *ObjectEntry) data.Object {
		obj := factory(meta)
		obj.FreezeAppend()
		return obj
	}
}
