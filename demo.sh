#!/usr/bin/env bash
# The reviewer's path in one script. Every step prints what it checks and stops on a failure.
#
#   ./demo.sh            build, verify the sample, run the tests, then a 2-minute live run
#   ./demo.sh sample     build and verify the AWS sample capture (offline)
#   ./demo.sh test       build and run every test (offline: mock venues on localhost)
#   ./demo.sh live [SECS] [MARKETS]
#                        a live Polymarket run (default 120 s, 2000 markets), then verify
#   ./demo.sh kalshi [SECS] [MARKETS]
#                        a live Kalshi run (default 120 s, 500 markets), then verify. Needs a key
#                        in .env (HOW_TO_RUN.md); KALSHI_DEMO=1 uses Kalshi's demo environment
#   ./demo.sh both [SECS]
#                        Polymarket and Kalshi at the same time, one process each, then verify both
set -euo pipefail
cd "$(dirname "$0")"

OUT=target/demo
step() { printf '\n== %s\n' "$*"; }

build() {
  command -v cargo >/dev/null || { echo "needs a stable Rust toolchain: https://rustup.rs"; exit 1; }
  step "build (release; the first build takes a few minutes)"
  cargo build --release --bins
  mkdir -p "$OUT"
}

sample() {
  step "verify the sample capture from the AWS run (sample/README.md)"
  gunzip -c sample/ob.sbe.gz > "$OUT/sample.sbe"
  ./target/release/verify "$OUT/sample.sbe" | tee "$OUT/sample-verify.txt"
  grep -q ' 0 did not' "$OUT/sample-verify.txt"
}

tests() {
  step "tests: unit, golden, 10 fault injections against a mock Polymarket, 3 against a mock Kalshi"
  cargo test --release
}

live() {
  local secs=${1:-120} markets=${2:-2000}
  step "live: $markets markets for $secs s against Polymarket (status line every 10 s)"
  ./target/release/collector --out "$OUT/live.sbe" --max-markets "$markets" --duration-secs "$secs"
  step "verify the live capture"
  ./target/release/verify "$OUT/live.sbe" | tee "$OUT/live-verify.txt"
  grep -q ' 0 did not' "$OUT/live-verify.txt"
  # An empty file also replays cleanly: require data. No data usually means the network blocks
  # the venue (some ISPs answer its DNS with their own block page; see GRADING.md).
  if grep -qE '^messages: +0 snapshots' "$OUT/live-verify.txt"; then
    echo "FAIL: the live run received no data. Check that ws-subscriptions-clob.polymarket.com"
    echo "resolves to Cloudflare (104.18.x / 172.64.x) from this network."
    exit 1
  fi
}

# The same engine with the Kalshi adapter. Kalshi signs every websocket handshake, so it needs a key.
kalshi() {
  local secs=${1:-120} markets=${2:-500}
  [ -f .env ] && { set -a; . ./.env; set +a; }
  if [ -z "${KALSHI_API_KEY_ID:-}" ] || [ -z "${KALSHI_PRIVATE_KEY_PATH:-}" ]; then
    echo "needs KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH in .env (HOW_TO_RUN.md, Kalshi)"; exit 1
  fi
  local urls=() env=production
  if [ "${KALSHI_DEMO:-0}" = 1 ]; then
    urls=(--ws-url wss://demo-api.kalshi.co/trade-api/ws/v2 --rest-url https://demo-api.kalshi.co/trade-api/v2)
    env=demo
  fi
  step "live: $markets Kalshi markets for $secs s ($env environment)"
  ./target/release/collector --venue kalshi ${urls[@]+"${urls[@]}"} --out "$OUT/kalshi.sbe" --max-markets "$markets" --duration-secs "$secs"
  step "verify the Kalshi capture"
  ./target/release/verify "$OUT/kalshi.sbe" | tee "$OUT/kalshi-verify.txt"
  grep -q ' 0 did not' "$OUT/kalshi-verify.txt"
}

# Ext 3: one binary, two adapters, run side by side. Each venue is its own process and file, so
# one venue failing cannot stop the other.
both() {
  local secs=${1:-120}
  step "Polymarket and Kalshi together for $secs s (logs in $OUT/both-*.log)"
  live "$secs" 2000 > "$OUT/both-polymarket.log" 2>&1 & local pm=$!
  kalshi "$secs" 500 > "$OUT/both-kalshi.log" 2>&1 & local ks=$!
  local fail=0
  wait $pm || { echo "Polymarket run failed: $OUT/both-polymarket.log"; fail=1; }
  wait $ks || { echo "Kalshi run failed: $OUT/both-kalshi.log"; fail=1; }
  grep -h -E 'snapshots matched|^OK|FAIL' "$OUT/live-verify.txt" "$OUT/kalshi-verify.txt" 2>/dev/null || true
  [ $fail = 0 ]
}

case "${1:-all}" in
  sample) build; sample ;;
  test) build; tests ;;
  live) build; live "${2:-}" "${3:-}" ;;
  kalshi) build; kalshi "${2:-}" "${3:-}" ;;
  both) build; both "${2:-}" ;;
  all) build; sample; tests; live ;;
  *) sed -n '2,13p' "$0"; exit 2 ;;
esac
step "done: every check passed"
