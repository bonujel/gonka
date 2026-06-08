package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Repro for PR1289 review finding #5 (clear-grace default drift).
//
// inference-chain/x/inference/types/params.go declares:
//
//	const DefaultDevshardInferenceClearGraceSeconds uint32 = 120
//
// with a comment claiming it "Mirrors devshard/types.DefaultInferenceClearGraceSeconds".
// It does not: the devshard-side compiled fallback below is 3600. When a session
// is created without a chain-supplied grace (runtime params missing / first-block
// Params failure / direct-chain fallback), NormalizeSessionConfig stamps 3600,
// while the chain governance default expects 120. Hosts in such a window bind a
// grace 30x larger than governance intends, shifting the seal/prune timing that
// finding #1 already shows is consensus-sensitive.
func TestRepro_ClearGraceDefaultDrift(t *testing.T) {
	const chainGovernanceDefault uint32 = 120 // params.DefaultDevshardInferenceClearGraceSeconds

	// The compiled devshard fallback constant.
	require.Equal(t, uint32(3600), uint32(DefaultInferenceClearGraceSeconds),
		"devshard compiled fallback")

	// What an unset session actually gets normalized to.
	got := NormalizeSessionConfig(SessionConfig{}, 5).InferenceClearGraceSeconds
	require.Equal(t, uint32(3600), got,
		"NormalizeSessionConfig fills the 3600 fallback when grace is unset")

	// The drift: the two "mirrored" defaults are not equal.
	require.NotEqual(t, chainGovernanceDefault, got,
		"DRIFT: chain governance default (120s) != devshard fallback bound at session create (3600s)")
}
