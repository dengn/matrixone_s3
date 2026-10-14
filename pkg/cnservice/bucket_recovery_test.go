package cnservice

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBucketWithdrawalPreservesRecoveryEvidence(t *testing.T) {
	old := pb.CNStoreInfo{
		ServiceAddress: "original:18000", ViewMetadataAdmissionGeneration: 42,
		ViewMetadataAdmissionSupported: true, ViewMetadataAdmissionReady: true,
		ViewMetadataIngressReady: true, ViewMetadataObservedEpoch: 9,
		ViewMetadataCatalogFencedEpoch: 9, ViewMetadataRevalidatedEpoch: 9,
		ViewMetadataRefreshSupported: true, CommandDeliveryAckSupported: true,
		PersistedExpressionProtocolVersion: 17,
		CatalogMetadataCapabilities:        &pb.CatalogMetadataCapabilities{PersistedExpressionProtocol: 17, BarrierParticipantProtocol: 1},
	}
	hb := bucketCNWithdrawal("cn", old)
	require.False(t, hb.ViewMetadataIngressReady)
	require.Equal(t, old.ViewMetadataAdmissionGeneration, hb.ViewMetadataAdmissionGeneration)
	require.Equal(t, old.ViewMetadataObservedEpoch, hb.ViewMetadataObservedEpoch)
	require.Equal(t, old.ViewMetadataCatalogFencedEpoch, hb.ViewMetadataCatalogFencedEpoch)
	require.Equal(t, old.PersistedExpressionProtocolVersion, hb.PersistedExpressionProtocolVersion)
	require.Equal(t, old.CatalogMetadataCapabilities, hb.CatalogMetadataCapabilities)
	require.Zero(t, hb.AckedCommandBatchID, "restoration must not acknowledge unseen scheduler commands")
	require.True(t, old.ViewMetadataIngressReady, "constructing a withdrawal must not mutate the recovered state")
}

func TestBucketRecoveryDoesNotRetireNormalCluster(t *testing.T) {
	// No bucket store is configured in normal CN service tests. The intentionally
	// absent HAKeeper client would panic if this path performed any RPC.
	s := &service{}
	require.NoError(t, s.retireBucketCNGeneration(t.Context()))
}
