# Performance build-out plan

Working plan for the last round before submission. Each step is measured before and after, and
nothing merges unless the correctness gates pass. Once the steps are done, this file becomes
`docs/PERFORMANCE.md` with the measured results in place of the targets.

The challenge ranks **complete** first and **time-accurate** second. Every step here serves one
of the two, or frees CPU and memory so the full universe stays complete on a small host.

## Correctness gates (every step)

1. `cargo test --release`: unit, property, golden hash and end-to-end tests, offline.
2. `cargo clippy --all-targets -- -D warnings`.
3. The book property test (`book.rs`, against a naive model) passes on the new representation.
4. A live run of at least 10 minutes. `verify` must report 0 mismatches, and `hash_ok` must be
   no lower than the baseline.

The template helpers in `src/util/` stay untouched.

## Step 0: measure first (the baselines)

| Harness | What it measures | How |
|---|---|---|
| `bench/mem` (new bin, offline) | bytes per token for the full universe | Load `universe.json` (about 560k tokens). Fill every book with a depth drawn from a real snapshot distribution. Count allocated bytes with a counting `GlobalAlloc`, and also report RSS. |
| `bench/cpu` (new bin, offline) | ns per frame for decode, and ns per event for the core | Replay `tests/fixtures` frames (captured live) through `decode → Core` in a loop, with a writer sink that discards. |
| Depth distribution | levels per side, p50/p90/p99 | Taken from the snapshots of one 2-minute live run (`verify` already parses them). |

Expected baseline: about 5.4 KB per token (3 GB / 560k).

## Step 1: memory (target ≤ 1.2 GB full universe, stretch ≤ 0.8 GB)

Order is by expected gain. Each item is its own commit with its own before/after row.

| # | Change | Where | Why | Expected |
|---|---|---|---|---|
| M1 | `mimalloc` as the global allocator | `main.rs`, `verify.rs` | Less fragmentation under constant small allocations, and faster malloc | −10–30% RSS |
| M2 | **Flat books.** Each side becomes a sorted `Vec<(Px, Qty)>`, with inline storage for small books (`SmallVec<[_; 4]>`). Bids are stored descending and asks ascending, so the best level is at index 0. | `book.rs`; callers in `emit.rs`, `polymarket.rs` (hash digest), `kalshi.rs`, `core` | A `BTreeMap` allocates a node of about 200 B per non-empty side. That is 6 trees per token (2 replicas plus the emitted book). Binary search over a contiguous array is also faster to apply. | The largest item, about −1 GB |
| M3 | Narrow price type. Price becomes `u32` in the adapter's tick units (Polymarket: 1e-6 fixed point fits a u32, since prices are ≤ 1); quantity stays `i64`. | `book.rs`, `fixed.rs` conversions at the edges | A level shrinks from 16 B to 12 B. Optional: only if M2's numbers say levels dominate. | −5–10% |
| M4 | Replicas stored inline: `HashMap<ConnId, Replica>` becomes `SmallVec<[(ConnId, Replica); 2]>` | `core/mod.rs`, `core/*.rs` | A hash table holding 2 entries costs about 400 B of overhead per token | −200–300 MB |
| M5 | **Symbol interning.** `Sym = Arc<str>` (a 77-char token id, three per token) becomes `SymId(u32)` into one interner. The writer resolves the id to `"<token>:bid"` at encode time. | `adapter/mod.rs`, `core`, `writer.rs`, adapters | Saves about 300 B per token, and every map key turns from a 77-byte string into a 4-byte int (see C1) | −150–250 MB |
| M6 | Lazy per-market hash state. `HashCheck.mkts` is keyed per (connection, market); allocate it only when the market first trades, and drop it after a quiet period. | `adapter/polymarket.rs` | Most of the about 280k markets rarely trade | −50–150 MB |
| M7 | Shrink the cold `Tok` fields: group the rarely used fields (quiet probe, split and rotation bookkeeping) into one boxed struct | `core/mod.rs` | `Tok` is about 250 B, held 560k times | −50 MB |

Report: bytes per token and RSS after each item, plus the full-universe RSS from the AWS run.

## Step 2: CPU on the hot path (target: core ns/event −40%, decode ns/frame −50%)

| # | Change | Why | Measured by |
|---|---|---|---|
| C1 | `rustc-hash` (FxHash) for every internal map, keyed by `SymId` / `ConnId` | Every map uses SipHash, run over 77-byte strings on every event | `bench/cpu` core ns/event |
| C2 | Release profile: `lto = "fat"`, `codegen-units = 1`. Keep unwinding, because the supervisor's panic handling needs it (D20). | Cross-crate inlining (serde, tungstenite, sha1) | `bench/cpu` |
| C3 | `-C target-cpu=x86-64-v3` in an opt-in `.cargo/config.toml` profile (`--profile fast`) | AVX2 for SHA1 and SIMD JSON. Not the default, so a grader's build works on any CPU. | `bench/cpu` |
| C4 | Decode with `sonic-rs` (SIMD, borrowed `&str`, so the exact decimal strings needed by the hash check stay intact) | The decode stage is serde_json on every frame | decode ns/frame; golden tests must stay bit-exact |
| C5 | Reuse the events buffer. Per-frame `Vec<NormEvent>`s come from a small pool and go back to it after the core consumes them. | One allocation per frame on 450 sockets | allocations per frame |
| C6 | SHA1 of the hash check: use the `sha1` crate's `asm` / SHA-NI feature where available | The hash check digests every book on every delta (the earlier hot spot) | `bench/cpu` |

C4 is kept only if it wins by more than 20% *and* the golden tests stay exact. Otherwise it is
documented as tried and reverted.

## Step 3: threading and backpressure

| # | Change | Why |
|---|---|---|
| T1 | **Thread budget.** `#[tokio::main]` defaults to one worker per CPU. On 4 vCPU that means 4 tokio workers plus 2 core threads plus writer plus finisher: 8 busy threads on 4 CPUs. Set the tokio workers to `CPUs − cores − 1` (minimum 1), with a `--io-threads` flag to override. | Oversubscription shows up as receive-side queueing, which inflates `recv_ts` |
| T2 | **Bounded queue sized to absorb disk stalls.** Size the writer channel in bytes, not batches, at about 256 MiB. The worst stall measured was 1.5 s × 8.5 MB/s ≈ 13 MB, so this is about 20× headroom. Add a `writer_queue_hwm` counter. The policy stays *never drop*: past the bound, cores wait, and the counter says so. | Answers "disk stalls reach the books" with a number |
| T3 | **CPU pinning** (Linux, `--pin`): core threads on dedicated CPUs, tokio and writer on the rest. A no-op on macOS. | Removes scheduler migration jitter from the books |
| T4 | Optional busy-poll for the core threads (`--spin`). Kept only if p99 `behind` improves on AWS by more than its CPU cost. | Wake-up latency of the core |

## Step 4: time accuracy (Linux, done on AWS)

| # | Change | Why |
|---|---|---|
| H1 | **Kernel receive timestamps.** The TCP stream is wrapped in an `AsyncRead` that calls `recvmsg` with `SO_TIMESTAMPNS`. The frame's `recv_ts` is the kernel arrival time of the read that completed it. Behind `--kernel-ts`, Linux only. | `recv_ts` stops including TLS, websocket parsing and tokio scheduling delay. The difference (app time − kernel time) becomes a new histogram, so we can say how much latency *our host* adds. |
| H2 | chrony against Amazon Time Sync (`169.254.169.123`). Optionally the ENA PTP hardware clock (`/dev/ptp0`) on instances that support it, for microsecond-level accuracy. Log `chronyc tracking` offset and error with every run. | Absolute latency |
| H3 | Socket options: `SO_RCVBUF` 4 MiB, `TCP_NODELAY` (for pings and resubscribes), optional `SO_BUSY_POLL` | Fewer receive-window stalls on bursts |
| H4 | Host: IRQ affinity for the ENA queues kept off the core CPUs, `irqbalance` off, gp3 at 250 MB/s or instance-store NVMe for output | Removes host noise from the p99 |
| H5 | **Region by measurement:** London, Ireland and N. Virginia side by side, then the winner for the long run | The biggest single latency lever |

## Order of work

1. Step 0 harnesses and baselines.
2. M1 → C1/C2 (cheap wins, small diffs).
3. M5 + C1 together (interning changes the map keys anyway).
4. M2 (+ M3 if it pays), then M4, M6, M7.
5. C4, C5, C6.
6. T1, T2, T3.
7. H1 behind a flag, tested on a Codespace (Linux).
8. AWS: region test, then a 60-minute full-universe run with every gate. That run is also the
   final validation of steps 1–3.

Each step is committed separately with its before/after numbers, and appended to the
ENGINEERING-LOG.

## Submission: how it will be run and graded

The brief says what they check. It is a data-collector role, and the brief stresses
*complete* and *time-accurate* data, written with the template helpers to stdout, with a clean
SIGINT. Expect a reviewer to:
1. unzip it, read the README, `cargo build --release`;
2. run `collector > out.sbe` for some minutes, press Ctrl-C, and check that the sockets and the
   file close cleanly;
3. read `out.sbe` with the template's reader, or with their own tooling on the standard format;
4. read the code (adapter trait, core, writer) and the extension write-ups, LATENCY above all;
5. possibly talk it through in an interview.

What the zip and the email must make easy:

| Item | What | Why |
|---|---|---|
| `README.md`, top 20 lines | A "Start here" block: 3 commands (build, run with Ctrl-C, `verify`), what the reviewer will see, and a headline results table (coverage, 0 replay mismatches, absolute latency from region R, memory per token, CPU) | Their first two minutes decide how carefully the rest gets read |
| `scripts/demo.sh` | build → test → 2-minute live run on 2000 markets → SIGINT → `verify` → a printed summary | One command reproduces the main claims |
| `sample/` | A 2-minute capture (a few MB), its `verify` report and the collector's stderr | They can inspect real output even if their network blocks Polymarket |
| `Dockerfile` | Multi-stage, static binary, `ulimit` set | Runs the same on a Mac or Linux reviewer box |
| `.github/workflows/ci.yml` | build, test, clippy on push | A green check on the public repo |
| Docs | ARCHITECTURE, DECISIONS, LATENCY, PERFORMANCE (this plan's results), DEPLOY, KALSHI, GRADING, ENGINEERING-LOG | Already in place, plus PERFORMANCE |
| Excluded | `target/`, `universe*.json`, `.env`, `*.pem`, large `.sbe` files, Codespace logs over a few MB | Size, and no secrets |

**Email** (to jobs@voluntas.io): short. A link to the public repo with the zip attached; three
headline numbers; one line on Polymarket instead of Kalshi (allowed by the brief, adapter built
and tested against a mock); a pointer to "Start here".

**A first run without `universe.json`** (the reviewer will not have one): discovery streams
pages, and shards open as pages arrive, so data flows within seconds. The demo checks this.

**Full-universe defaults on a laptop:** after Step 1 the full universe needs about 1 GB and
about 30 GB of disk an hour. The README says so up front, and the demo uses `--max-markets`.

## Not doing (and why)

- Kernel bypass (DPDK, AF_XDP), custom TLS, NIC hardware timestamps on the far end: weeks of
  work. The venue is behind Cloudflare, so the gain is unmeasurable from our side.
- `panic = "abort"`: it would break `--supervise`'s in-process panic handling (D20).
- Changing `src/util/`: the challenge requires the template helpers as they are.
