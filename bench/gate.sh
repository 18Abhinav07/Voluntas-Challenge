#!/usr/bin/env bash
# Runs inside a Codespace: the full universe under --supervise with 5-minute rotation, for DUR
# seconds. Faults, each optional: a 15 s SIGSTOP of the child at STOP_AT, and a kill -9 of the
# child at KILL_AT (the supervisor restarts it and recovers the .part file). Finished files are
# verified (niced) and deleted as they land. Every 60 s: child RSS and CPU.
set -u
cd "$(dirname "$0")/.."
REV=${REV:?}; DUR=${DUR:-1200}; ROT=${ROT:-5}; STOP_AT=${STOP_AT:-0}; KILL_AT=${KILL_AT:-0}; NAME=${NAME:-gate}
exec > $NAME.log 2>&1
command -v cargo >/dev/null || { curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"; }
git fetch -q origin && git checkout -q "$REV" || { echo CHECKOUT_FAILED; exit 1; }
echo "== host $(nproc) cpus, $(free -m | awk '/Mem/{print $2}') MB, $(df -h . | tail -1 | awk '{print $4}') free, region $(curl -s ipinfo.io/region)"
git log --oneline -1
cargo build --release --bins || { echo BUILD_FAILED; exit 1; }
rm -rf out verify.txt res.txt; mkdir -p out

verify_done() {
  for f in out/*.sbe; do
    [ -e "$f" ] || continue
    r=$(nice -n 19 ./target/release/verify "$f" | grep -E "span|messages|replay|resets|FAIL|OK" | tr '\n' ' ')
    echo "verify $(basename $f) $(du -h $f | cut -f1) $r" >> verify.txt
    rm -f "$f"
  done
}

echo "== start $(date -u +%H:%M:%S) dur=$DUR rot=$ROT stop_at=$STOP_AT kill_at=$KILL_AT"
NO_COLOR=1 ./target/release/collector --supervise --out out/ob.sbe --rotate-mins $ROT --universe-cache u.json > collect-$NAME.log 2>&1 &
sup=$!; t0=$(date +%s); stopped=0; killed=0
while kill -0 $sup 2>/dev/null; do
  sleep 10
  el=$(( $(date +%s) - t0 ))
  child=$(pgrep -P $sup | head -1)
  if [ -n "$child" ] && [ $(( el % 60 )) -lt 10 ]; then
    echo "$el rss_mb=$(( $(awk '/VmRSS/{print $2}' /proc/$child/status) / 1024 )) cpu=$(ps -o %cpu= -p $child)" >> res.txt
  fi
  if [ $STOP_AT -gt 0 ] && [ $stopped = 0 ] && [ $el -ge $STOP_AT ] && [ -n "$child" ]; then
    stopped=1; echo "SIGSTOP child 15 s at t=$el"; kill -STOP $child; sleep 15; kill -CONT $child
  fi
  if [ $KILL_AT -gt 0 ] && [ $killed = 0 ] && [ $el -ge $KILL_AT ] && [ -n "$child" ]; then
    killed=1; echo "kill -9 child at t=$el"; kill -9 $child
  fi
  [ $el -ge $DUR ] && kill -TERM $sup
  verify_done
done
wait $sup; echo "supervisor exit $? at $(date -u +%H:%M:%S)"; verify_done
L() { cat collect-$NAME.log; }
echo "-- resources (t rss cpu)"; cat res.txt | tr '\n' ' '; echo
echo "-- verify"; cat verify.txt
echo "-- mismatches total: $(grep -oE '[0-9]+ did not' verify.txt | awk '{s+=$1} END{print s+0}') in $(wc -l < verify.txt) files"
echo "-- clock"; L | grep -E 'clock offset|host clock' | cut -c29-200
echo "-- supervisor"; L | grep -E 'supervisor:|recovered crashed|removed crashed' | cut -c29-240
echo "-- warn/error"; L | grep -E "WARN|ERROR" | cut -c29-140 | sed 's/conn=[0-9]*//; s/[0-9.]*s$//' | sort | uniq -c | sort -rn | head -10
echo "-- status (last 3)"; L | grep ' status' | tail -3 | cut -c29-600
echo "-- done"; L | grep ' done ' | cut -c29-3000
echo RUN_DONE
