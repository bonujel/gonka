package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/types"
)

// --- Repro harness for PR1289 review finding #1 (CRITICAL state-root fork) ---
//
// These tests are intentionally self-contained: two or more StateMachines that
// share the SAME escrow / group / config / user are driven through the SAME
// diff stream. The only thing that varies is WHEN (at which local nonce) a host
// seals an inference -- which in production is decided by each host's local wall
// clock (host.go:606 `now := time.Now()` gating emitTierC/emitTerminal seals).
//
// If sealing were outside the consensus state root, every machine below would
// compute an identical root. The asserts show they do NOT.

func forkReproSM(t *testing.T, escrowID string, user *signing.Secp256k1Signer, hosts []*signing.Secp256k1Signer, group []types.SlotAssignment, config types.SessionConfig) *StateMachine {
	t.Helper()
	verifier := signing.NewSecp256k1Verifier()
	store := testutil.MustMemoryStore(t, escrowID, user.Address(), config, group, 100000)
	sm, err := NewStateMachine(escrowID, config, group, 100000, user.Address(), verifier, store,
		WithVersion(types.DevshardStateRootAndProtocolVersion))
	require.NoError(t, err)
	return sm
}

// forkReproDriveToFinished applies the identical nonce 1..3 stream that ends with
// inference 1 in StatusFinished (mirrors driveSealInferenceToFinished).
func forkReproDriveToFinished(t *testing.T, sm *StateMachine, escrowID string, hosts []*signing.Secp256k1Signer) {
	t.Helper()

	_, err := sm.ApplyLocal(1, []*types.DevshardTx{txStart(&types.MsgStartInference{
		InferenceId: 1, PromptHash: []byte("prompt"), Model: "llama", InputLength: 100, MaxTokens: 50, StartedAt: 1000,
	})})
	require.NoError(t, err)

	execSig := testutil.SignExecutorReceipt(t, hosts[1], escrowID, 1, []byte("prompt"), "llama", 100, 50, 1000, 2000)
	_, err = sm.ApplyLocal(2, []*types.DevshardTx{txConfirm(&types.MsgConfirmStart{
		InferenceId: 1, ExecutorSig: execSig, ConfirmedAt: 2000,
	})})
	require.NoError(t, err)

	finish := &types.MsgFinishInference{
		InferenceId: 1, ResponseHash: []byte("response"), InputTokens: 10, OutputTokens: 20, ExecutorSlot: 1, EscrowId: escrowID,
	}
	finish.ProposerSig = testutil.SignProposerTx(t, hosts[1], finish)
	_, err = sm.ApplyLocal(3, []*types.DevshardTx{txFinish(finish)})
	require.NoError(t, err)
}

// forkReproAdvance applies an identical nonce-4 diff (start of an unrelated
// inference id 2) so two machines reach the same final nonce.
func forkReproAdvance(t *testing.T, sm *StateMachine) {
	t.Helper()
	_, err := sm.ApplyLocal(4, []*types.DevshardTx{txStart(&types.MsgStartInference{
		InferenceId: 4, PromptHash: []byte("p2"), Model: "llama", InputLength: 100, MaxTokens: 50, StartedAt: 4000,
	})})
	require.NoError(t, err)
}

func forkReproFiveHosts(t *testing.T) []*signing.Secp256k1Signer {
	t.Helper()
	return []*signing.Secp256k1Signer{
		testutil.MustGenerateKey(t), testutil.MustGenerateKey(t), testutil.MustGenerateKey(t),
		testutil.MustGenerateKey(t), testutil.MustGenerateKey(t),
	}
}

// Finding #1, claim (a): the sequencer never seals mid-session (it builds diffs
// via ApplyLocalBestEffort), while a host seals when its local wall-clock gate
// fires. At the SAME nonce the host's recomputed root != the sequencer's claimed
// PostStateRoot, which in production triggers ErrPostStateRootMismatch + rollback.
func TestRepro_SequencerVsHost_RootMismatch(t *testing.T) {
	hosts := forkReproFiveHosts(t)
	user := testutil.MustGenerateKey(t)
	group := testutil.MakeGroup(hosts)
	config := testutil.DefaultConfig(len(hosts))
	const escrowID = "escrow-repro-a"

	// Sequencer: applies the same diffs, NEVER seals mid-session.
	seq := forkReproSM(t, escrowID, user, hosts, group, config)
	forkReproDriveToFinished(t, seq, escrowID, hosts)
	forkReproAdvance(t, seq)
	rootSeq, err := seq.ComputeStateRoot()
	require.NoError(t, err)

	// Host: identical diffs, but its wall-clock grace elapsed for inference 1, so
	// it sealed id 1 before applying nonce 4 (sealNonce = 3).
	host := forkReproSM(t, escrowID, user, hosts, group, config)
	forkReproDriveToFinished(t, host, escrowID, hosts)
	require.NoError(t, host.SealInference(1))
	forkReproAdvance(t, host)
	rootHost, err := host.ComputeStateRoot()
	require.NoError(t, err)

	require.NotEqual(t, rootSeq, rootHost,
		"host that sealed mid-session computes a different root than the non-sealing sequencer "+
			"-> the sequencer's diff.PostStateRoot for this nonce would be rejected (ErrPostStateRootMismatch)")
}

// Finding #1, claim (b): the sealed nonce is folded into SealedAcc, and that nonce
// is each host's LOCAL progress when its wall-clock gate fires. Two honest hosts
// that cross the gate at different local nonces seal the SAME inference at
// different sealNonces and therefore fork the root -- even though they end at the
// same nonce with identical applied diffs.
func TestRepro_TwoHostsDifferentLocalNonce_RootFork(t *testing.T) {
	hosts := forkReproFiveHosts(t)
	user := testutil.MustGenerateKey(t)
	group := testutil.MakeGroup(hosts)
	config := testutil.DefaultConfig(len(hosts))
	const escrowID = "escrow-repro-b"

	// Host EARLY: gate fired at nonce 3 -> seal id1 (sealNonce=3), then advance to 4.
	early := forkReproSM(t, escrowID, user, hosts, group, config)
	forkReproDriveToFinished(t, early, escrowID, hosts)
	require.NoError(t, early.SealInference(1))
	forkReproAdvance(t, early)

	// Host LATE: advance to nonce 4 first, gate fired there -> seal id1 (sealNonce=4).
	late := forkReproSM(t, escrowID, user, hosts, group, config)
	forkReproDriveToFinished(t, late, escrowID, hosts)
	forkReproAdvance(t, late)
	require.NoError(t, late.SealInference(1))

	require.Equal(t, early.LatestNonce(), late.LatestNonce(), "both hosts end at the same nonce")

	rootEarly, err := early.ComputeStateRoot()
	require.NoError(t, err)
	rootLate, err := late.ComputeStateRoot()
	require.NoError(t, err)

	require.NotEqual(t, rootEarly, rootLate,
		"same final nonce + identical diffs, only the local seal nonce differs (3 vs 4) -> forked root")
}
