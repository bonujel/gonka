package keeper_test

import (
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/require"

	"github.com/productscience/inference/x/inference/keeper"
	"github.com/productscience/inference/x/inference/types"
)

// Repro for PR1289 review finding #3 (fee snapshot not validated on chain).
//
// CreateDevshardEscrow snapshots the governance fee schedule onto the escrow
// (CreateDevshardFee + FeePerNonce, proto fields 10/11). At settlement the chain
// only enforces totalCost + msg.Fees <= escrow.Amount and trusts the host quorum
// signature over the state root. It never recomputes the expected fee from the
// snapshot, so a settlement whose Fees bear no relation to
// (CreateDevshardFee + nonce*FeePerNonce) is accepted.
func TestRepro_Settlement_DoesNotValidateFeeSnapshot(t *testing.T) {
	sdk.GetConfig().SetBech32PrefixForAccount("gonka", "gonka")

	keys, slots := generateDevshardKeys(t, keeper.DevshardGroupSize)

	const nonce uint64 = 42 // buildSettlementTestData's fixed nonce
	escrow := types.DevshardEscrow{
		Id:                1,
		Creator:           "gonka1creator",
		Amount:            7_000_000_000,
		Slots:             slots,
		CreateDevshardFee: 1_000_000, // snapshotted governance fee schedule
		FeePerNonce:       1_000,
	}

	// What the snapshot says the fees SHOULD be for this settlement.
	honestFees := escrow.CreateDevshardFee + nonce*escrow.FeePerNonce // 1_042_000

	// An arbitrary value that violates the schedule but still fits under Amount
	// (cost = 16 * 100M = 1.6G, so fees up to ~5.4G are within Amount).
	wrongFees := uint64(123_456_789)
	require.NotEqual(t, honestFees, wrongFees, "test fixture: wrong fee must differ from snapshot fee")

	hostStats := makeHostStats(keeper.DevshardGroupSize, 100_000_000)
	msg := buildSettlementTestData(t, escrow, keys, hostStats, wrongFees)

	err := keeper.VerifyDevshardSettlement(escrow, msg, testDevshardEscrowParams(), nil)

	// Bug: accepted. If the chain validated the snapshot it would reject
	// wrongFees != honestFees. It does not.
	require.NoError(t, err,
		"chain accepted settlement fees=%d that violate the escrow snapshot "+
			"(create=%d + nonce=%d * per_nonce=%d => honest=%d); no independent fee recompute",
		wrongFees, escrow.CreateDevshardFee, nonce, escrow.FeePerNonce, honestFees)
}
