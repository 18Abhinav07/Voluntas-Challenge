#!/usr/bin/env bash
# Runs inside a Codespace: Ext 4 TCP source-port experiment. Every socket goes to one edge IP,
# so only the 4-tuple's source port differs. Replicas 0..FIXED-1 bind the same port in every
# round; the rest take a new ephemeral port each round. ROUNDS short runs, each a fresh set of
# connections, with a gap longer than Linux's 60 s TIME_WAIT so a fixed port can be reused.
# If the port decides the path, fixed slots keep their speed from round to round and
# ephemeral slots do not (bench/port_an.py).
set -u
cd "$(dirname "$0")"
REV=${REV:?}; ROUNDS=${ROUNDS:-6}; RD=${RD:-240}; GAP=${GAP:-75}; FIXED=${FIXED:-6}; EPH=${EPH:-6}
MARKETS=${MARKETS:-400}; BASE=${BASE:-41001}
exec > port.log 2>&1
command -v cargo >/dev/null || { curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"; }
git fetch -q origin && git checkout -q "$REV" || { echo CHECKOUT_FAILED; exit 1; }
echo "== host $(nproc) cpus, region $(curl -s ipinfo.io/region 2>/dev/null)"; git log --oneline -1
cargo build --release --bins || { echo BUILD_FAILED; exit 1; }
EDGE=${EDGE:-$(getent ahostsv4 ws-subscriptions-clob.polymarket.com | awk '{print $1}' | sort -u | head -1)}
PORTS=$(seq -s, $BASE $((BASE + FIXED - 1)))
echo "== edge $EDGE fixed_ports $PORTS ephemeral $EPH rounds $ROUNDS x ${RD}s gap ${GAP}s markets $MARKETS"
rm -rf out; mkdir -p out
for k in $(seq 1 $ROUNDS); do
  echo "== round $k start $(date -u +%H:%M:%S)"
  NO_COLOR=1 ./target/release/collector --out out/ob$k.sbe --duration-secs $RD --universe-cache u.json \
    --max-markets $MARKETS --replicas $((FIXED + EPH)) --tokens-per-conn 2000 --core-threads 1 --recycle-mins 0 \
    --lat-detail --edge-ip $EDGE --src-ports $PORTS > collect-$k.log 2>&1
  echo "== round $k exit $? at $(date -u +%H:%M:%S)"
  ./target/release/verify out/ob$k.sbe | tail -3
  grep -c "connection lost" collect-$k.log | sed 's/^/lost /'
  [ $k -lt $ROUNDS ] && sleep $GAP
done
python3 port_an.py $FIXED collect-*.log > summary.txt
echo DONE
