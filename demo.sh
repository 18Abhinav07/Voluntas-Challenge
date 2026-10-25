#!/usr/bin/env bash
# The reviewer's path in one script. Every step prints what it checks and stops on a failure.
#
#   ./demo.sh            build, verify the sample, run the tests, then a 2-minute live run
#   ./demo.sh sample     build and verify the AWS sample capture (offline)
#   ./demo.sh test       build and run every test (offline: mock venues on localhost)
#   ./demo.sh live [SECS] [MARKETS]
#                        a live Polymarket run (default 120 s, 2000 markets), then verify
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

case "${1:-all}" in
  sample) build; sample ;;
  test) build; tests ;;
  live) build; live "${2:-}" "${3:-}" ;;
  all) build; sample; tests; live ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
step "done: every check passed"
