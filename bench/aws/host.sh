#!/usr/bin/env bash
# Runs on an EC2 host (Ubuntu 24.04), driven by bench/aws/run.sh. Every phase writes into
# ~/results and ends with a marker line (<PHASE>_OK / <PHASE>_FAILED) in ~/results/markers, so
# the driver can poll over ssh and a dropped ssh session loses nothing.
#
#   host.sh setup main|probe  packages, chrony on Amazon Time Sync (+ PHC if present), limits;
#                             main also installs Rust and builds bins, examples and tests
#   host.sh smoke             45 s on 20 markets, then verify: proves the venue is reachable
#   host.sh probe START       region probe at epoch START: 2,000 markets, --lat-detail, 10 min
#   host.sh main              tests, offline bench, 60-min full universe, 2-min sample
#   host.sh tune              A/B rounds, two collectors at once on the same markets (TUNE.md):
#                             speed-aware source, replica count, failover window, busy polling,
#                             and a kill -9 of an unsupervised run
#
# DRY=1 shortens every duration (a dry run on any Linux box); NTP overrides the time server.
set -u
cd "$HOME"
R=$HOME/results; mkdir -p "$R"
DRY=${DRY:-0}
NTP=${NTP:-169.254.169.123:123}
BIN=$HOME/src/target/release
[ -x "$HOME/bin/collector" ] && BIN=$HOME/bin
mark() { echo "$1 $(date -u +%FT%TZ)" >> "$R/markers"; }
dur() { if [ "$DRY" = 1 ]; then echo "$2"; else echo "$1"; fi; }
L() { sed 's/\x1b\[[0-9;]*m//g' "$1"; }

env_capture() { # host facts, clock state and the path to the venue
  {
    echo "== $(date -u +%FT%TZ)"
    echo "instance $(cat /sys/devices/virtual/dmi/id/product_name 2>/dev/null) $(curl -s -m 2 -H "X-aws-ec2-metadata-token: $(curl -s -m 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/dynamic/instance-identity/document | jq -c '{instanceType,region,availabilityZone}' 2>/dev/null)"
    echo "kernel $(uname -r)"; nproc; free -m | head -2; df -h "$HOME" | tail -1
    lscpu | grep -E 'Model name|Thread|Core|Socket|NUMA node|MHz'
    for c in /sys/devices/system/cpu/cpu[0-9]*; do echo "$(basename $c) siblings $(cat $c/topology/thread_siblings_list)"; done
    echo "-- clock"; chronyc -n sources 2>&1; chronyc tracking 2>&1
    ls -l /dev/ptp* 2>&1; cat /sys/class/ptp/ptp*/clock_name 2>/dev/null
    echo "-- venue"
    for h in ws-subscriptions-clob.polymarket.com clob.polymarket.com gamma-api.polymarket.com; do
      echo "$h $(getent ahostsv4 $h | awk '{print $1}' | sort -u | tr '\n' ' ')"
      curl -s -o /dev/null -m 10 -w "  https connect %{time_connect}s tls %{time_appconnect}s first byte %{time_starttransfer}s code %{http_code}\n" "https://$h/" || echo "  curl failed"
    done
    for e in $(getent ahostsv4 ws-subscriptions-clob.polymarket.com | awk '{print $1}' | sort -u); do
      echo "ping $e $(ping -c 20 -i 0.2 -q $e 2>&1 | tail -1)"
    done
    echo "traceroute: $(command -v tracepath >/dev/null && tracepath -n -m 12 ws-subscriptions-clob.polymarket.com 2>&1 | tail -4 | tr '\n' '|')"
  } >> "$R/env.txt" 2>&1
}

setup() {
  local role=$1
  exec >> "$R/setup.log" 2>&1
  echo "== setup $role $(date -u +%FT%TZ)"
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get update -q && sudo apt-get install -y -q chrony jq sysstat iputils-tracepath build-essential pkg-config \
    || { mark SETUP_FAILED; exit 1; }
  sudo apt-get install -y -q linux-tools-$(uname -r) || echo "no perf for this kernel (profile skipped)"
  # Amazon Time Sync, and the ENA hardware clock when the instance exposes one.
  if [ -n "${NO_CHRONY:-}" ]; then echo "chrony left alone (NO_CHRONY)"; else
    conf=/etc/chrony/chrony.conf
    grep -q 169.254.169.123 $conf || echo "server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4" | sudo tee -a $conf
    if ls /dev/ptp0 >/dev/null 2>&1 && grep -qi ena /sys/class/ptp/ptp0/clock_name 2>/dev/null; then
      grep -q 'refclock PHC' $conf || echo "refclock PHC /dev/ptp0 poll 0 delay 0.000010 prefer" | sudo tee -a $conf
    fi
    sudo systemctl restart chrony || sudo service chrony restart || true
    chronyc waitsync 30 0.0005 || echo "chrony not synced within 5 min (continuing)"
  fi
  sudo sysctl -q -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 || true
  if [ "$role" = main ]; then
    command -v cargo >/dev/null || [ -x "$HOME/.cargo/bin/cargo" ] || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    . "$HOME/.cargo/env"
    cd "$HOME/src" || { mark SETUP_FAILED; exit 1; }
    t=$(date +%s)
    cargo build --release --bins --examples || { mark SETUP_FAILED; exit 1; }
    echo "build $(( $(date +%s) - t )) s"
    cargo test --release --no-run || { mark SETUP_FAILED; exit 1; }
    ls -l target/release/collector target/release/verify
  else
    [ -x "$HOME/bin/collector" ] || { echo "no binaries in ~/bin"; mark SETUP_FAILED; exit 1; }
  fi
  env_capture
  mark SETUP_OK
}

smoke() {
  exec >> "$R/smoke.log" 2>&1
  echo "== smoke $(date -u +%FT%TZ)"
  ulimit -n 65536 2>/dev/null || ulimit -n 4096
  rm -rf smoke; mkdir -p smoke
  NO_COLOR=1 $BIN/collector --out smoke/ob.sbe --duration-secs 45 --max-markets 20 --universe-cache smoke/u.json \
    --ntp-server $NTP > smoke/collect.log 2>&1
  echo "collector exit $?"
  L smoke/collect.log | grep -E 'status|clock|done|ERROR' | tail -6 | cut -c1-400
  if $BIN/verify smoke/ob.sbe > smoke/verify.txt 2>&1 && grep -q OK smoke/verify.txt \
     && L smoke/collect.log | grep ' status' | tail -1 | grep -qE 'sourced=[1-9]'; then
    tail -5 smoke/verify.txt; mark SMOKE_OK
  else
    cat smoke/verify.txt; mark SMOKE_FAILED
  fi
}

cpu_groups() { # cumulative CPU ticks per thread group of process $1
  for t in /proc/$1/task/*; do
    c=$(cat $t/comm 2>/dev/null) || continue
    s=$(cut -d')' -f2- $t/stat 2>/dev/null | awk '{print $12+$13}') || continue
    echo "$c $s"
  done | awk '{g=$1; if(g ~ /^tokio/) g="tokio"; else if(g=="collector") g="main"; else if(g ~ /^core/) g="core"; a[g]+=$2} END{for(k in a) print k, a[k]}' | sort
}
# Bytes received on the default-route interface. Read from sysfs, not /proc/net/dev: the
# column layout there shifts with the interface name's padding.
NET_IF=$(ip route show default 2>/dev/null | awk '{print $5; exit}')
net_rx() { cat /sys/class/net/${NET_IF:-eth0}/statistics/rx_bytes 2>/dev/null || echo 0; }
disk_wr() { awk '$3 ~ /^(nvme[0-9]+n[0-9]+|xvd[a-z]|sd[a-z]|vd[a-z])$/ {s+=$10} END{print s*512}' /proc/diskstats; }

verify_done() { # $1 dir: verify finished files (niced), record, delete
  for f in "$1"/*.sbe; do
    [ -e "$f" ] || continue
    local t=$(date +%s)
    r=$(nice -n 19 $BIN/verify "$f" 2>&1 | grep -E "span|messages|replay|resets|latency|crossed|FAIL|OK|mismatch" | tr '\n' ' ')
    echo "verify $(basename $f) $(du -h $f | cut -f1) $(( $(date +%s) - t ))s $r" >> "$R/verify-$2.txt"
    rm -f "$f"
  done
}

# Samples every 10 s while process $1 lives: CPU per thread group, RSS, network in, disk out.
sample_loop() {
  local pid=$1 name=$2 sup=${3:-}
  local prev_cpu prev_rx prev_wr child t0=$(date +%s)
  child=$pid; [ -n "$sup" ] && child=$(pgrep -P $sup | head -1)
  prev_cpu=$(cpu_groups $child); prev_rx=$(net_rx); prev_wr=$(disk_wr)
  while kill -0 ${sup:-$pid} 2>/dev/null; do
    sleep 10
    if [ -n "$sup" ]; then
      local c; c=$(pgrep -P $sup | head -1); [ -n "$c" ] || continue
      # A restarted child starts its counters at zero: re-base and skip one sample.
      [ "$c" != "$child" ] && { child=$c; prev_cpu=$(cpu_groups $child); prev_rx=$(net_rx); prev_wr=$(disk_wr); continue; }
    fi
    local el=$(( $(date +%s) - t0 )) now rx wr rss
    now=$(cpu_groups $child); rx=$(net_rx); wr=$(disk_wr)
    rss=$(( $(awk '/VmRSS/{print $2}' /proc/$child/status 2>/dev/null || echo 0) / 1024 ))
    echo "$el rss_mb=$rss rx_mbps=$(( (rx - prev_rx) * 8 / 10 / 1000000 )) wr_mbs=$(( (wr - prev_wr) / 10 / 1000000 )) load=$(cut -d' ' -f1 /proc/loadavg) $(join <(echo "$prev_cpu") <(echo "$now") 2>/dev/null | awk '{printf "%s=%d ", $1, ($3-$2)/10}')" >> "$R/res-$name.txt"
    prev_cpu=$now; prev_rx=$rx; prev_wr=$wr
    [ $(( el % 300 )) -lt 10 ] && echo "$el $(chronyc -n tracking 2>/dev/null | grep -E 'System time|RMS offset|Root dispersion' | tr -s ' ' | tr '\n' '|')" >> "$R/clock-$name.txt"
    [ -n "${HOOK:-}" ] && $HOOK $el $child
    [ -n "$sup" ] && verify_done out-$name $name # rotated runs only: an unrotated file is still open
  done
}

summarize() { # $1 name, $2 collector log
  local n=$1 log=$2
  {
    echo "===== $n"
    echo "-- resources: steady mean (t>180) and peak; CPU in % of one CPU per thread group"
    awk '$1>180 {for(i=2;i<=NF;i++){split($i,a,"="); s[a[1]]+=a[2]; c[a[1]]++}} END{for(k in s) printf "%s=%.1f ", k, s[k]/c[k]; print ""}' "$R/res-$n.txt"
    awk '{for(i=2;i<=NF;i++){split($i,a,"="); if(a[2]+0>m[a[1]]) m[a[1]]=a[2]}} END{for(k in m) printf "%s=%s ", k, m[k]; print ""}' "$R/res-$n.txt"
    echo "-- verify"; cat "$R/verify-$n.txt" 2>/dev/null
    echo "-- mismatches total: $(grep -oE '[0-9]+ did not' "$R/verify-$n.txt" 2>/dev/null | awk '{s+=$1} END{print s+0}') in $(wc -l < "$R/verify-$n.txt" 2>/dev/null || echo 0) files"
    echo "-- host delay (app - kernel), every window"; L $log | grep 'host delay' | cut -c29-400
    echo "-- clock"; L $log | grep -E 'clock offset|host clock' | cut -c29-220; cat "$R/clock-$n.txt" 2>/dev/null
    echo "-- status (every 10th, and the last)"; L $log | grep 'collector: status ' | awk 'NR%10==1 {print} {l=$0} END{print l}' | cut -c29-1500
    echo "-- thread budget"; L $log | grep -E 'thread budget|core threads started|pinned' | cut -c29-300
    echo "-- edges / per-socket latency (last 3 windows)"; L $log | grep -E 'websocket edges|edge latency|socket latency' | tail -4 | cut -c29-1500
    echo "-- warn/error"; L $log | grep -E "WARN|ERROR" | cut -c29-140 | sed 's/conn=[0-9]*//; s/[0-9.]*s$//' | sort | uniq -c | sort -rn | head -12
    echo "slow consumer closes: $(grep -c 'slow consumer' $log)"
    echo "writer backpressure: $(grep -c 'writer backpressure' $log)"
    echo "-- supervisor"; L $log | grep -E 'supervisor:|recovered crashed|removed crashed' | cut -c29-240
    echo "-- done"; L $log | grep -E ' done' | cut -c29-3000
  } >> "$R/summary.txt"
}

probe() {
  local start=${1:?} d; d=$(dur 600 60)
  exec >> "$R/probe.log" 2>&1
  ulimit -n 65536 2>/dev/null || ulimit -n 4096
  rm -rf out-probe; mkdir -p out-probe
  local w=$(( start - $(date +%s) )); [ $w -gt 0 ] && sleep $w
  echo "== probe start $(date -u +%FT%TZ) (late by $(( $(date +%s) - start )) s) dur=$d"
  NO_COLOR=1 $BIN/collector --out out-probe/probe.sbe --duration-secs $d --max-markets 2000 --lat-detail --kernel-ts \
    --ntp-server $NTP --universe-cache out-probe/u.json > "$R/collect-probe.log" 2>&1 &
  local pid=$!
  sample_loop $pid probe
  wait $pid; echo "collector exit $?"
  verify_done out-probe probe
  summarize probe "$R/collect-probe.log"
  if grep -q ' 0 did not' "$R/verify-probe.txt" 2>/dev/null; then mark PROBE_OK; else mark PROBE_FAILED; fi
}

# Phase hook for the full run: one perf profile at mid-run, and a kill -9 of the child near the end.
main_hook() {
  local el=$1 child=$2
  if [ "$PERF_AT" -gt 0 ] && [ ! -e "$R/.perf" ] && [ $el -ge "$PERF_AT" ] && command -v perf >/dev/null; then
    touch "$R/.perf"
    ( sudo perf record -F 199 -g -p $child -o perf.data -- sleep 30 >/dev/null 2>&1
      sudo perf report -i perf.data --no-children --sort comm,symbol --stdio 2>/dev/null | grep -v '^#' | grep -v '^$' | head -80 > "$R/perf-top.txt"
      sudo rm -f perf.data ) &
  fi
  if [ "$KILL_AT" -gt 0 ] && [ ! -e "$R/.killed" ] && [ $el -ge "$KILL_AT" ]; then
    touch "$R/.killed"; echo "kill -9 child $child at t=$el" >> "$R/main.log"; kill -9 $child
  fi
}

main() {
  exec >> "$R/main.log" 2>&1
  . "$HOME/.cargo/env"
  ulimit -n 65536 2>/dev/null || ulimit -n 4096
  cd "$HOME/src"
  echo "== tests $(date -u +%FT%TZ)"
  if cargo test --release > "$R/tests.log" 2>&1; then mark TESTS_OK; else mark TESTS_FAILED; fi
  grep -E '^test result|FAILED|panicked' "$R/tests.log" >> "$R/main.log"
  echo "== bench $(date -u +%FT%TZ)"
  { ./target/release/examples/bench cpu; ./target/release/examples/bench mem $(dur 283000 20000); } > "$R/bench.txt" 2>&1 \
    && mark BENCH_OK || mark BENCH_FAILED

  local d rot; d=$(dur 3600 240); rot=$(dur 10 1)
  PERF_AT=$(dur 1800 90); KILL_AT=$(dur 3300 200); export PERF_AT KILL_AT
  cd "$HOME"; rm -rf out-main; mkdir -p out-main
  echo "== main start $(date -u +%FT%TZ) dur=$d rot=$rot perf_at=$PERF_AT kill_at=$KILL_AT"
  NO_COLOR=1 $BIN/collector --supervise --out out-main/ob.sbe --rotate-mins $rot --pin --kernel-ts \
    --ntp-server $NTP --universe-cache out-main/u.json > "$R/collect-main.log" 2>&1 &
  local sup=$! t0=$(date +%s)
  sleep 2
  ( while kill -0 $sup 2>/dev/null; do sleep 5; [ $(( $(date +%s) - t0 )) -ge $d ] && { kill -TERM $sup; break; }; done ) &
  HOOK=main_hook sample_loop $sup main $sup
  wait $sup; echo "supervisor exit $? at $(date -u +%FT%TZ)"
  verify_done out-main main
  summarize main "$R/collect-main.log"
  cp out-main/u.json "$R/universe-size.txt" 2>/dev/null && jq length "$R/universe-size.txt" > "$R/universe-markets.txt" 2>/dev/null; rm -f "$R/universe-size.txt"
  if grep -q ' 0 did not' "$R/verify-main.txt" && ! grep -qE '[1-9][0-9]* did not' "$R/verify-main.txt"; then mark MAIN_OK; else mark MAIN_FAILED; fi

  echo "== sample $(date -u +%FT%TZ)"
  rm -rf "$R/sample"; mkdir -p "$R/sample"
  NO_COLOR=1 $BIN/collector --out "$R/sample/ob.sbe" --duration-secs $(dur 120 30) --max-markets 500 --kernel-ts \
    --ntp-server $NTP --universe-cache out-main/u.json > "$R/sample/collector.log" 2>&1
  $BIN/verify "$R/sample/ob.sbe" > "$R/sample/verify.txt" 2>&1 && mark SAMPLE_OK || mark SAMPLE_FAILED
  du -h "$R/sample/ob.sbe" >> "$R/main.log"
  env_capture
  mark ALL_DONE
}

# One A/B round: two collectors at the same time, each with its own sockets, on the same markets.
ab() { # $1 round $2 secs $3 markets $4 A-args $5 B-args
  local n=$1 d=$2 m=$3 x
  rm -rf out-$n; mkdir -p out-$n/A out-$n/B
  cp tune-u.json out-$n/A/u.json; cp tune-u.json out-$n/B/u.json
  echo "== $n $(date -u +%FT%TZ) dur=$d markets=$m A=[$4] B=[$5]"
  for x in A B; do
    local a; [ $x = A ] && a=$4 || a=$5
    NO_COLOR=1 $BIN/collector --out out-$n/$x/ob.sbe --duration-secs $d --max-markets $m --kernel-ts \
      --ntp-server $NTP --universe-cache out-$n/$x/u.json $a > "$R/collect-$n-$x.log" 2>&1 &
    eval "pid_$x=\$!"
  done
  sample_loop $pid_A $n-A & local sa=$!
  sample_loop $pid_B $n-B & local sb=$!
  wait $pid_A $pid_B; wait $sa $sb
  for x in A B; do verify_done out-$n/$x $n-$x; done
  rm -rf out-$n
}

# Sequential arms for a host-wide setting (a sysctl cannot differ between two processes).
seq_arm() { # $1 name $2 secs $3 markets
  rm -rf out-$1; mkdir -p out-$1; cp tune-u.json out-$1/u.json
  echo "== $1 $(date -u +%FT%TZ) dur=$2 markets=$3 busy_read=$(sysctl -n net.core.busy_read) busy_poll=$(sysctl -n net.core.busy_poll)"
  NO_COLOR=1 $BIN/collector --out out-$1/ob.sbe --duration-secs $2 --max-markets $3 --kernel-ts \
    --ntp-server $NTP --universe-cache out-$1/u.json > "$R/collect-$1.log" 2>&1 &
  local pid=$!
  sample_loop $pid $1
  wait $pid
  verify_done out-$1 $1; rm -rf out-$1
}

tune() {
  exec >> "$R/tune.log" 2>&1
  ulimit -n 65536 2>/dev/null || ulimit -n 4096
  local full=$(dur 1200 120) part=$(dur 900 90) seqd=$(dur 360 45) m=${TUNE_MARKETS:-2000}
  echo "== tune $(date -u +%FT%TZ)"
  # One discovery, shared, so both arms of every round start on the same markets.
  rm -rf out-prep; mkdir -p out-prep
  NO_COLOR=1 $BIN/collector --out out-prep/ob.sbe --duration-secs $(dur 120 60) --universe-cache tune-u.json > "$R/collect-prep.log" 2>&1
  rm -rf out-prep
  [ -s tune-u.json ] || { echo "no universe cache"; mark TUNE_FAILED; exit 1; }
  echo "universe $(jq length tune-u.json) markets"

  ab r1-fast-full $full 0 "--lat-detail" "--fast-source"; mark R1_DONE
  ab r2-replicas $part $m "--fast-source" "--fast-source --replicas 4"; mark R2_DONE
  ab r3-lag $part $m "--lat-detail" "--lat-detail --lag-ms 1000"; mark R3_DONE

  local br=$(sysctl -n net.core.busy_read) bp=$(sysctl -n net.core.busy_poll)
  seq_arm r4-poll-off1 $seqd $m
  sudo sysctl -q -w net.core.busy_read=50 net.core.busy_poll=50
  seq_arm r4-poll-on $seqd $m
  sudo sysctl -q -w net.core.busy_read=$br net.core.busy_poll=$bp
  seq_arm r4-poll-off2 $seqd $m; mark R4_DONE

  # kill -9, unsupervised: what was written before the kill must replay.
  rm -rf out-r5; mkdir -p out-r5; cp tune-u.json out-r5/u.json
  echo "== r5-kill $(date -u +%FT%TZ)"
  NO_COLOR=1 $BIN/collector --out out-r5/ob.sbe --max-markets 500 --kernel-ts --ntp-server $NTP \
    --universe-cache out-r5/u.json > "$R/collect-r5-kill.log" 2>&1 &
  local pid=$!
  sleep $(dur 120 30); kill -9 $pid; wait $pid 2>/dev/null
  echo "killed; file $(stat -c %s out-r5/ob.sbe 2>/dev/null) bytes" >> "$R/verify-r5-kill.txt"
  verify_done out-r5 r5-kill; rm -rf out-r5
  if grep -qE '^verify .* 0 did not' "$R/verify-r5-kill.txt" && ! grep -q 'messages: +0 snapshots' "$R/verify-r5-kill.txt"; then mark R5_OK; else mark R5_FAILED; fi
  env_capture
  mark TUNE_DONE
}

case "${1:-}" in
  setup) setup "${2:?main|probe}" ;;
  smoke) smoke ;;
  probe) probe "${2:?START epoch}" ;;
  main) main ;;
  tune) tune ;;
  *) echo "usage: host.sh setup main|probe | smoke | probe START | main | tune"; exit 2 ;;
esac
