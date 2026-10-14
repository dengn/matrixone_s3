// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectstoragefs"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

// retireBucketCNGeneration runs before this CN allocates or publishes its new
// generation. Bucket attachments have private local transports: the previous
// incarnation in the restored control plane cannot have an ingress in this
// process. Writers additionally acquired the bucket's CAS lease before any
// native role starts; readers cannot publish outside their local overlay.
//
// Publish the normal, committed ingress withdrawal instead of waiting for that
// restored incarnation's 30-second liveness timeout. Catalog/epoch admission and
// allocation still follow the unchanged native protocol. Normal cluster restart
// must never use this shortcut: its old incarnation may actually still be live.
func (s *service) retireBucketCNGeneration(ctx context.Context) error {
	if objectstoragefs.Current() == nil {
		return nil
	}
	state, err := s._hakeeperClient.GetClusterState(ctx)
	if err != nil {
		return err
	}
	previous, ok := state.CNState.Stores[s.cfg.UUID]
	if !ok || previous.ViewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if !previous.ViewMetadataAdmissionReady && !previous.ViewMetadataIngressReady {
		return nil
	}
	_, err = s._hakeeperClient.SendCNHeartbeat(ctx, bucketCNWithdrawal(s.cfg.UUID, previous))
	if err == nil {
		s.logger.Info("retired restored bucket CN ingress", zap.Uint64("generation", previous.ViewMetadataAdmissionGeneration))
	}
	return err
}

func bucketCNWithdrawal(id string, old pb.CNStoreInfo) pb.CNStoreHeartbeat {
	return pb.CNStoreHeartbeat{
		UUID: id, ServiceAddress: old.ServiceAddress, SQLAddress: old.SQLAddress,
		LockServiceAddress: old.LockServiceAddress, QueryAddress: old.QueryAddress,
		ShardServiceAddress: old.ShardServiceAddress, Role: old.Role,
		InitWorkState: old.WorkState.String(), TaskServiceCreated: old.TaskServiceCreated,
		ConfigData: old.ConfigData, Resource: old.Resource, CommitID: old.CommitID,
		CommandDeliveryAckSupported:     old.CommandDeliveryAckSupported,
		ViewMetadataAdmissionSupported:  old.ViewMetadataAdmissionSupported,
		ViewMetadataAdmissionGeneration: old.ViewMetadataAdmissionGeneration,
		ViewMetadataObservedEpoch:       old.ViewMetadataObservedEpoch,
		ViewMetadataCatalogFencedEpoch:  old.ViewMetadataCatalogFencedEpoch,
		ViewMetadataRefreshSupported:    old.ViewMetadataRefreshSupported,
		ViewMetadataRevalidatedEpoch:    old.ViewMetadataRevalidatedEpoch,
		// A withdrawal preserves the old decoder floor and catalog evidence. It
		// neither grants the new incarnation capability nor acknowledges commands.
		PersistedExpressionProtocolVersion: old.PersistedExpressionProtocolVersion,
		CatalogMetadataCapabilities:        old.CatalogMetadataCapabilities,
		CatalogMetadataAck:                 old.CatalogMetadataAck,
		ViewMetadataIngressReady:           false,
	}
}
