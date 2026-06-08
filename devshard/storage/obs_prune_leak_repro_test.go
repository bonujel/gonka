package storage

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Repro for PR1289 review finding #2 (Postgres prune leaks validation-obs
// partitions). Requires Docker (testcontainers); skips otherwise.
//
// ensurePartition (postgres.go) creates 8 per-epoch partitions, INCLUDING the
// three validation-obs parents. pruneBefore / pgPartitionEpoch only enumerate 5
// of them, so the three obs partitions are never DROPped and accumulate one
// table per epoch forever.
//
// The existing TestPostgres_PruneBefore_* tests cannot see this because their
// assertion helper (listDevshardPartitions) hardcodes the SAME incomplete
// 5-parent list as the bug. This test queries the obs parents directly.
func TestRepro_PruneBefore_LeaksValidationObsPartitions(t *testing.T) {
	pg := newTestPostgres(t)

	// CreateSession runs ensurePartition, which creates all 8 partitions
	// (incl. the three *_validation_obs ones) for each epoch.
	require.NoError(t, pg.CreateSession(paramsForEpoch("old", 100)))
	require.NoError(t, pg.CreateSession(paramsForEpoch("new", 105)))

	// Sanity: epoch 100's obs partitions exist before pruning.
	require.Equal(t, []string{
		"devshard_inference_validation_obs_epoch_100",
		"devshard_sealed_validation_obs_epoch_100",
		"devshard_slot_validation_obs_epoch_100",
	}, listObsPartitions(t, pg.pool, 100))

	// Prune everything before epoch 102. Epoch 100 must be fully reclaimed.
	require.NoError(t, pg.pruneBefore(102))

	// The bug: the obs partitions for epoch 100 survive (leak). This assertion
	// FAILS on the current code, proving finding #2.
	require.Empty(t, listObsPartitions(t, pg.pool, 100),
		"validation-obs partitions for a pruned epoch must be DROPped, but pruneBefore omits the three obs parents -> per-epoch leak")
}

// TestRepro_PgPartitionEpoch_OmitsObsParents is the no-Docker proof of finding
// #2. pruneBefore only drops a partition when pgPartitionEpoch recognizes its
// name. ensurePartition creates obs partitions every epoch, yet pgPartitionEpoch
// does not recognize any of the three obs parents -> they can never be dropped.
func TestRepro_PgPartitionEpoch_OmitsObsParents(t *testing.T) {
	// Recognized parents: pruneBefore can reclaim these.
	for _, name := range []string{
		"devshard_sessions_epoch_100",
		"devshard_diffs_epoch_100",
		"devshard_signatures_epoch_100",
		"devshard_snapshots_epoch_100",
		"devshard_sealed_inferences_epoch_100",
	} {
		ep, ok := pgPartitionEpoch(name)
		require.True(t, ok, "%s should be recognized as prunable", name)
		require.Equal(t, uint64(100), ep)
	}

	// The three validation-obs parents are created by ensurePartition every
	// epoch but are NOT recognized here, so pruneBefore never drops them.
	for _, name := range []string{
		"devshard_slot_validation_obs_epoch_100",
		"devshard_inference_validation_obs_epoch_100",
		"devshard_sealed_validation_obs_epoch_100",
	} {
		_, ok := pgPartitionEpoch(name)
		require.False(t, ok,
			"LEAK: %s is created per-epoch but pgPartitionEpoch does not recognize it, "+
				"so pruneBefore can never DROP it", name)
	}
}

// listObsPartitions returns the validation-obs partition tables for a given
// epoch that currently exist. Unlike listDevshardPartitions, it queries the
// three obs parents the bug omits.
func listObsPartitions(t *testing.T, pool *pgxpool.Pool, epochID uint64) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT c.relname
		FROM pg_class c
		JOIN pg_inherits i ON i.inhrelid = c.oid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname IN (
			'devshard_slot_validation_obs',
			'devshard_inference_validation_obs',
			'devshard_sealed_validation_obs'
		)
		ORDER BY c.relname
	`)
	require.NoError(t, err)
	defer rows.Close()

	suffix := "_epoch_" + strconv.FormatUint(epochID, 10)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		if strings.HasSuffix(name, suffix) {
			names = append(names, name)
		}
	}
	require.NoError(t, rows.Err())
	sort.Strings(names)
	if names == nil {
		return []string{}
	}
	return names
}
