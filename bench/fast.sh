#!/usr/bin/env bash
# Runs inside a Codespace: Ext 4 speed-aware source A/B. Two collectors at the same time on
# the same markets, each with its own REPS sockets: A picks sources as before (--lat-detail),
# B with --fast-source. Compare `src_behind` (how late the written events are) and verify both.
set -u
cd "$(dirname "$0")"
REV=${REV:?}; DUR=${DUR:-1200}; REPS=${REPS:-4}; MARKETS=${MARKETS:-400}
exec > fast.log 2>&1
command -v cargo >/dev/null || { curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"; }
git fetch -q origin && git checkout -q "$REV" || { echo CHECKOUT_FAILED; exit 1; }
echo "== host $(nproc) cpus, region $(curl -s ipinfo.io/region 2>/dev/null)"; git log --oneline -1
cargo build --release --bins || { echo BUILD_FAILED; exit 1; }
rm -rf outA outB; mkdir -p outA outB
echo "== start $(date -u +%H:%M:%S) dur=$DUR reps=$REPS markets=$MARKETS"
run() { NO_COLOR=1 ./target/release/collector --out out$1/ob.sbe --duration-secs $DUR --universe-cache u$1.json \
  --max-markets $MARKETS --replicas $REPS --tokens-per-conn 2000 --core-threads 1 $2 > collect$1.log 2>&1; }
run A --lat-detail & run B --fast-source & wait
echo "== exit at $(date -u +%H:%M:%S)"
for x in A B; do echo "== verify $x"; ./target/release/verify out$x/ob.sbe | tail -8; done
echo DONE
