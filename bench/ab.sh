#!/usr/bin/env bash
# Runs inside a Codespace: one A/B arm. Builds REV once, then runs each config in turn.
# Both arms are started together, so they see the same market and cross the same rotations.
# Per config: CPU per thread group and stalls every 10 s, finished files verified (niced) and
# deleted as they land, then a summary block. No perf: nothing perturbs the measurement.
set -u
cd "$(dirname "$0")"
ARM=${ARM:?}; REV=${REV:?}
exec > ab-$ARM.log 2>&1
command -v cargo >/dev/null || { curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; . "$HOME/.cargo/env"; }
git fetch -q origin && git checkout -q "$REV" || { echo CHECKOUT_FAILED; exit 1; }
echo "== host $(nproc) cpus, $(free -m | awk '/Mem/{print $2}') MB, $(df -h . | tail -1 | awk '{print $4}') free"; git log --oneline -1
cargo build --release --bins || { echo BUILD_FAILED; exit 1; }
# Every config starts at START + its offset (epoch seconds), so both arms stay in lockstep.
START=${START:?}
align() { local w=$(( START + $1 - $(date +%s) )); [ $w -gt 0 ] && sleep $w; echo "late by $(( $(date +%s) - START - $1 )) s"; }

cpu_groups() {
  for t in /proc/$pid/task/*; do
    c=$(cat $t/comm 2>/dev/null) || continue
    s=$(cut -d')' -f2- $t/stat 2>/dev/null | awk '{print $12+$13}') || continue
    echo "$c $s"
  done | awk '{g=$1; if(g ~ /^tokio/) g="tokio"; else if(g=="collector") g="main"; else if(g ~ /^core/) g="core"; a[g]+=$2} END{for(k in a) print k, a[k]}' | sort
}

verify_done() { # verify and delete finished files
  for f in out/*.sbe; do
    [ -e "$f" ] || continue
    r=$(nice -n 19 ./target/release/verify "$f" | grep -E "messages|replay|FAIL|OK|mismatch" | tr '\n' ' ')
    echo "verify $(basename $f) $(du -h $f | cut -f1) $r" >> verify.txt
    rm -f "$f"
  done
}

run() { # $1 start offset s, $2 name, $3 duration s, $4 rotate mins, $5 SIGSTOP at (0 = none), rest = extra args
  local off=$1 name=$2 dur=$3 rot=$4 stop=$5; shift 5
  rm -rf out verify.txt cpu.txt; mkdir -p out
  align $off
  echo "== $name start $(date -u +%H:%M:%S) dur=$dur rot=$rot stop=$stop args=$*"
  ./target/release/collector --out out/ob.sbe --rotate-mins $rot --duration-secs $dur --universe-cache u.json "$@" > collect-$name.log 2>&1 &
  pid=$!; t0=$(date +%s); stopped=0
  prev=$(cpu_groups)
  while kill -0 $pid 2>/dev/null; do
    sleep 10
    now=$(cpu_groups); el=$(( $(date +%s) - t0 ))
    echo "$el $(join <(echo "$prev") <(echo "$now") | awk '{printf "%s=%d ", $1, ($3-$2)/10}')" >> cpu.txt
    prev=$now
    if [ $stop -gt 0 ] && [ $stopped = 0 ] && [ $el -ge $stop ]; then
      stopped=1; echo "SIGSTOP 15 s at t=$el"; kill -STOP $pid; sleep 15; kill -CONT $pid
    fi
    verify_done
  done
  wait $pid; echo "exit $?"; verify_done
  L() { sed 's/\x1b\[[0-9;]*m//g' collect-$name.log; }
  echo "-- cpu (mean % of one core, steady = after t=120)"
  awk '$1>120 {for(i=2;i<=NF;i++){split($i,a,"="); s[a[1]]+=a[2]; n[a[1]]++}} END{for(k in s) printf "%s=%.0f ", k, s[k]/n[k]; print ""}' cpu.txt
  echo "-- cpu peak"; awk '{for(i=2;i<=NF;i++){split($i,a,"="); if(a[2]>m[a[1]]) m[a[1]]=a[2]}} END{for(k in m) printf "%s=%d ", k, m[k]; print ""}' cpu.txt
  echo "-- verify"; cat verify.txt
  echo "-- stalls per status line"; L | grep ' status' | grep -oE 'stalls=[0-9]+' | tr '\n' ' '; echo
  echo "-- blocked ms per 30 s window (all sockets)"; L | grep 'conn stats' | grep -oE 'all: [^|]*' | grep -oE 'stall[0-9]+' | tr '\n' ' '; echo
  echo "-- hottest socket per window"; L | grep 'conn stats' | grep -oE 'top rx \[[^ ,]*' | cut -c9- | tr '\n' ' '; echo
  echo "-- warn/error"; L | grep -E "WARN|ERROR" | cut -c29-140 | sed 's/conn=[0-9]*//; s/[0-9.]*s$//' | sort | uniq -c | sort -rn | head -8
  echo "slow consumer closes: $(grep -c "slow consumer" collect-$name.log)"
  echo "writer backpressure: $(grep -c "writer backpressure" collect-$name.log)"
  echo "splits: $(grep -c "splitting hot shard" collect-$name.log)"; L | grep "splitting hot shard" | cut -c29-260 | head -6
  echo "-- done"; L | grep ' done' | cut -c29-2000
  echo "== $name end"
}

run 0 full-rot5 900 5 600
run 1200 full-rot15 2100 15 1500
run 3600 small-rot5 900 5 0 --max-markets 4000
echo RUN_DONE
