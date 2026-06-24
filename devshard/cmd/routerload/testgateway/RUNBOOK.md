# A/B runbook — 1 escrow vs 2 escrows on a self-hosted gateway

Goal: reproduce the big-customer concurrency issue and measure where the 429
ceiling is, with **1 escrow** vs **2 escrows**, on a gateway you run yourself —
**zero impact on the production router**.

This pairs the self-hosted gateway (the same `devshardctl` image prod runs) with
the [routerload](../README.md) load generator. The escrow count is a gateway-side
setting; routerload just drives traffic and records `max_inflight` + 429s.

---

## 0. Prerequisites (the real gates — sort these first)

1. **Allowlisted creator address.** The `gonka1…` address derived from
   `DEVSHARD_PRIVATE_KEY` must be on-chain in
   `devshard_escrow_params.allowed_creator_addresses`. You **cannot self-add** —
   it needs a governance vote ("Become a broker"), OR get an
   already-allowlisted+funded creator key from the team.
2. **Funds.** Mainnet escrows use real ngonka (deposit ≥ `min_amount`, ~5 GONKA
   in the docs example, + gas + fees). For 2 escrows budget ~2× deposit.
   👉 Cheapest path: run this against a **testnet** node instead of mainnet
   (swap the `NODE_*` URLs in the env file).
3. Linux host + Docker + `inferenced` v0.2.13.

Confirm the allowlist + `min_amount` BEFORE funding (quickstart §2.4):
```bash
inferenced query inference devshard-escrow-params --node "$NODE_RPC" -o json | jq
# check allowed_creator_addresses contains $DEVSHARD_CREATOR, and min_amount
```

---

## A. Stand up the gateway

```bash
cd devshard/cmd/routerload/testgateway
cp config.devshard.env.example config.devshard.env
# fill DEVSHARD_PRIVATE_KEY / DEVSHARD_API_KEYS / DEVSHARD_ADMIN_API_KEY,
# and set the GATEWAY_* limit knobs to MATCH PRODUCTION (ask the team).
chmod 600 config.devshard.env
source config.devshard.env

sudo docker compose pull
sudo docker compose up -d
sudo docker compose ps                 # expect: running / healthy
curl -fsS http://127.0.0.1:18080/v1/status | jq '{runtimes, capacity: .capacity.models}'
```

---

## B. Create ONE escrow + open API access

```bash
source config.devshard.env
CREATE_JSON=$(curl -sS -X POST http://127.0.0.1:18080/v1/admin/escrows \
  -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"amount\":5000000000,\"model_id\":\"$DEVSHARD_MODEL\",\"private_key\":\"$DEVSHARD_PRIVATE_KEY\",\"chain_id\":\"$CHAIN_ID\",\"register\":true}")
echo "$CREATE_JSON" | jq .
export ESCROW_ID=$(echo "$CREATE_JSON" | jq -r '.escrow_id')

# models are admin_only until you enable api_key access:
curl -sS -X POST http://127.0.0.1:18080/v1/admin/settings \
  -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" -H "Content-Type: application/json" \
  -d "{\"default_request_max_tokens\":3072,\"request_max_tokens_cap\":4096,\"model_limits\":[{\"model_id\":\"$DEVSHARD_MODEL\",\"access_mode\":\"api_key\"}]}"

curl -fsS http://127.0.0.1:18080/v1/admin/devshards \
  -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" | jq '.[].escrow_id'   # expect 1 entry
```

---

## C. Load test with 1 escrow

```bash
export ROUTER_API_KEY="$DEVSHARD_API_KEYS"      # the user key from the env file
cd ../                                            # devshard/cmd/routerload
go run . \
  -url http://127.0.0.1:18080/v1/chat/completions \
  -model "$DEVSHARD_MODEL" \
  -rps 5,10,15,20,25 -dur 60s \
  -max-tokens 4096 -prompt-tokens 2000 -timeout 90s \
  -label 1escrow -detail detail-1escrow.csv
```
Watch the first-429 body on stderr — confirm it's `too many concurrent requests`.

---

## D. Add a SECOND escrow (pool mode), then re-test

```bash
source testgateway/config.devshard.env
curl -sS -X POST http://127.0.0.1:18080/v1/admin/escrows \
  -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" -H "Content-Type: application/json" \
  -d "{\"amount\":5000000000,\"model_id\":\"$DEVSHARD_MODEL\",\"private_key\":\"$DEVSHARD_PRIVATE_KEY\",\"chain_id\":\"$CHAIN_ID\",\"register\":true}" | jq .
curl -fsS http://127.0.0.1:18080/v1/admin/devshards \
  -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" | jq '.[].escrow_id'   # expect 2 entries
```
The pooled `/v1/chat/completions` now load-balances each request across both
escrows (picker scores by in-flight vs capacity). Re-run the SAME load:
```bash
go run . -url http://127.0.0.1:18080/v1/chat/completions -model "$DEVSHARD_MODEL" \
  -rps 5,10,15,20,25 -dur 60s -max-tokens 4096 -prompt-tokens 2000 -timeout 90s \
  -label 2escrow -detail detail-2escrow.csv
```

---

## E. Compare

```bash
column -t -s, routerload-1escrow.csv
column -t -s, routerload-2escrow.csv
```
Read across the two CSVs:
- **`max_inflight` at the first 429** = the concurrency ceiling. Does 2 escrows
  raise it? If yes ≈2×, the bottleneck is the per-escrow/gateway cap and a pool
  fixes it. If 2 escrows barely helps, the ceiling is downstream (GPU/host
  capacity) — adding escrows won't fix it, you need more hosts.
- **`first429@s`**: immediate (steady-state cap) vs delayed (cumulative — escrow
  balance/nonce depletion).
- **highest RPS with `429+timeout` < ~1% over the full step** = sustainable RPS.

---

## F. Things that bite a single escrow (why 2 helps)

- **Concurrent cap**: `GATEWAY_MAX_CONCURRENT_REQUESTS` (+ `DEVSHARD_CAPACITY_AWARE_LIMITS=on`,
  dynamic) — the likely source of `too many concurrent requests`.
- **Nonce budget**: the multi-escrow gateway stops routing new chat to an escrow
  at **~19,800 nonce** (≈19,800 requests; doc says this will be raised). At 9 req/s
  that's ~36 min — matches the customer's "degrade after ~20-25 min" cliff.
- **Balance depletion**: usable balance < 1,000,000 ngonka ⇒ escrow marked
  depleted (checked ~every 30s).
- **Rotation is OFF by default**: a depleted escrow is NOT auto-replaced — the
  gateway just stops using it. Enable `escrow_rotation` (admin settings) for
  always-on, or run a pool big enough for the test window.

---

## G. Cleanup — finalize & settle to get funds back (do NOT skip on mainnet)

Per-escrow in pool mode uses the `/devshard/{id}/…` prefix:
```bash
for id in $(curl -fsS http://127.0.0.1:18080/v1/admin/devshards \
    -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" | jq -r '.[].escrow_id'); do
  curl -sS -X POST "http://127.0.0.1:18080/devshard/$id/v1/finalize" \
    -H "Authorization: Bearer $DEVSHARD_ADMIN_API_KEY" -o "settle-$id.json"
  inferenced tx inference settle-devshard-escrow "settle-$id.json" \
    --from devshard-create --keyring-backend test \
    --keyring-dir "$INFERENCED_KEYRING" --home "$INFERENCED_HOME" --node "$NODE_RPC" -y
done
sudo docker compose down
```
Settlement returns the unused deposit to the creator wallet.

---

## Also run: bypass the gateway to split blame

Run routerload a 3rd time against the **production** `api.gonkascan.com/v1` path
(customer view) and against a **direct dapi** endpoint if available. If the
end-to-end path 429s but your self-hosted gateway at the same in-flight does not,
the limit is in that prod layer (e.g. new-api), not in devshard/escrow.
