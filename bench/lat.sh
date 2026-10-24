#!/usr/bin/env bash
# Runs inside a Codespace: Ext 4 many-connections / per-edge experiment. One core, one shard,
# N replica sockets on the same tokens, so every socket sees the same events.
set -u
cd "$(dirname "$0")"
REV=${REV:?}; DUR=${DUR:-1200}; REPS=${REPS:-8}; MARKETS=${MARKETS:-400}
exec > lat.log 2>&1
command -v cargo >/dev/null || { curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"; }
git fetch -q origin && git checkout -q "$REV" || { echo CHECKOUT_FAILED; exit 1; }
echo "== host $(nproc) cpus, region $(curl -s ipinfo.io/region 2>/dev/null)"; git log --oneline -1
cargo build --release --bins || { echo BUILD_FAILED; exit 1; }
getent ahostsv4 ws-subscriptions-clob.polymarket.com | awk '{print $1}' | sort -u | sed 's/^/edge /'
for e in $(getent ahostsv4 ws-subscriptions-clob.polymarket.com | awk '{print $1}' | sort -u); do
  echo "ping $e $(ping -c 20 -i 0.2 -q $e 2>&1 | tail -1)"
done
rm -rf out; mkdir -p out
echo "== start $(date -u +%H:%M:%S) dur=$DUR reps=$REPS markets=$MARKETS"
NO_COLOR=1 ./target/release/collector --out out/ob.sbe --duration-secs $DUR --universe-cache u.json \
  --max-markets $MARKETS --replicas $REPS --tokens-per-conn 2000 --core-threads 1 --lat-detail > collect.log 2>&1
echo "== exit $? at $(date -u +%H:%M:%S)"
./target/release/verify out/ob.sbe | tail -8
echo DONE
