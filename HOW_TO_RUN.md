# How to run it

Every way the collector has been run, from quickest to most production-like. Each one ends with
`verify`, which replays the output file and exits non-zero on any mismatch.

| Way | Needs | Good for |
|---|---|---|
| [1. Local](#1-local) | Rust | Checking it works, offline tests, a short live run |
| [2. Docker](#2-docker) | Docker | Running it without installing Rust |
| [3. GitHub Codespace](#3-github-codespace) | A GitHub account | A Linux host in the browser, 4 cores |
| [4. AWS, scripted](#4-aws-scripted) | AWS credentials | The measured full-universe run, launched and torn down from a laptop |
| [5. Any Linux host, as a service](#5-any-linux-host-as-a-service) | A 4 vCPU / 8 GB VM | Running it continuously |
| [Kalshi](#kalshi) | A Kalshi API key | The challenge's venue, on any of the above |

## 1. Local

Install stable Rust from [rustup.rs](https://rustup.rs) (edition 2024, so 1.85 or newer). macOS and
Linux both work. `--kernel-ts` and `--pin` are Linux only.

```bash
./demo.sh
```

This builds, replays the committed sample, runs the offline tests and collects 2,000 live
Polymarket markets for two minutes, written through stdout. Output goes to `target/demo/`. The
pieces on their own:

| Command | What it does |
|---|---|
| `./demo.sh sample` | Build and replay the committed sample capture. No network |
| `./demo.sh test` | Build and run every test against mock venues. No network |
| `./demo.sh live [SECS] [MARKETS]` | Live Polymarket run (default 120 s, 2,000 markets), then verify |
| `./demo.sh kalshi [SECS] [MARKETS]` | Live Kalshi run (default 120 s, 500 markets), then verify |
| `./demo.sh both [SECS]` | Polymarket and Kalshi at the same time, one process each |
| `./demo.sh help` | This list |

By hand:

```bash
cargo build --release --bins
```

The output goes to stdout by default (`--out -`), so redirect it or pipe it on:

```bash
./target/release/collector --max-markets 2000 --duration-secs 120 > ob.sbe
```

```bash
./target/release/verify ob.sbe
```

To write a file directly instead, pass `--out`. Rotation (`--rotate-mins`) needs this form:

```bash
./target/release/collector --out ob.sbe --max-markets 2000 --duration-secs 120
```

The collector refuses to write binary to a terminal, so a bare run with no redirect and no
`--out` stops with a message saying so. Status lines go to stderr every 10 s. Ctrl-C stops it
cleanly: it closes the websockets, writes the last chunk and closes the output.

The full universe is about 550k tokens. That needs about 2 GB of RAM, 2 cores and 30 GB of disk an
hour; leave out `--max-markets` and `--duration-secs` for it.

## 2. Docker

```bash
docker build -t collector .
```

```bash
docker run --rm -v "$PWD/out:/out" collector --out /out/ob.sbe --max-markets 2000 --duration-secs 120
```

```bash
docker run --rm -v "$PWD/out:/out" --entrypoint verify collector /out/ob.sbe
```

`docker stop` sends SIGTERM, which closes the file the same way Ctrl-C does. For the full universe,
add `--ulimit nofile=65536`.

## 3. GitHub Codespace

The repo has a `.devcontainer/` that asks for a 4-core machine with Rust installed.

1. On the GitHub page: **Code → Codespaces → Create codespace on cloud-run**.
2. In its terminal, run `./demo.sh`.

The 2.5-hour soak ran on a Codespace like this (Azure West Europe, 4 vCPU, 16 GB): 320M updates,
0 mismatches. Its disk is slow (8 stalls of 1–1.5 s in 2.5 h) and its clock is not synced, so the
latency numbers come from AWS.

## 4. AWS, scripted

[bench/aws/run.sh](bench/aws/run.sh) launches the hosts, sets them up, runs the measured gates,
pulls the results and terminates everything. It needs the AWS CLI v2 and a login:

```bash
aws login
```

```bash
bench/aws/run.sh preflight
```

```bash
caffeinate -i bench/aws/run.sh all
```

`preflight` checks the identity, quotas and AMI and launches nothing. `all` runs:
- a c7i.xlarge in London for the 60-minute full-universe run;
- two smaller probes in Ireland and Virginia for the 10-minute region comparison.

The results land in `bench/aws-<date>/`. Every resource is tagged `Project=voluntas-run`, and each
instance shuts itself down after a fixed lifetime even if the laptop goes away. If a run is
interrupted, clean up with:

```bash
bench/aws/run.sh teardown
```

On a host already set up, `MAIN_SECS` sets the length of the main run and `host.sh kalshi` runs
the same phase against Kalshi's demo (it reads `~/kalshi.env` and `~/kalshi.pem`). The two-hour
run used `MAIN_SECS=7200 bench/aws/host.sh main` in London and `MAIN_SECS=7200 bench/aws/host.sh
kalshi` in Virginia, for about $0.72 in total: [bench/aws-2h-2026-10-25/](bench/aws-2h-2026-10-25/).

The full run took about 75 minutes and cost about $0.50. `bench/aws/run.sh tune` runs the A/B rounds
from docs/LATENCY.md instead. [docs/appendix/AWS-RUN.md](docs/appendix/AWS-RUN.md) has the plan
and the gates.

## 5. Any Linux host, as a service

Use 4 dedicated vCPU, 8 GB and a disk that sustains 50 MB/s. On AWS that is a c7i.xlarge; London
(eu-west-2) had the lowest latency to Polymarket. Burstable types (t3, e2) run out of credits.

```bash
sudo apt-get update && sudo apt-get install -y build-essential chrony git
```

```bash
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
```

```bash
git clone https://github.com/18Abhinav07/Voluntas-Challenge collector && cd collector && cargo build --release --bins
```

Point chrony at the provider's time source so `recv − exch` means something. On AWS that is
`server 169.254.169.123 prefer iburst`. Then a systemd unit keeps the supervisor alive:

```ini
# /etc/systemd/system/collector.service
[Unit]
Description=Order-book collector
After=network-online.target chrony.service
Wants=network-online.target

[Service]
WorkingDirectory=/opt/collector
ExecStart=/opt/collector/target/release/collector --supervise --pin --kernel-ts --out /data/ob.sbe --rotate-mins 10
KillSignal=SIGINT
TimeoutStopSec=60
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

- `--supervise` restarts the collector within a second if it dies, and keeps every whole chunk of
  the file it was writing.
- `--rotate-mins 10` writes `ob.<time>.sbe` files. A file is `.sbe.part` until it is finished, so
  anything ending in `.sbe` is safe to verify and move off the host.
- Logs go to the journal: `journalctl -u collector -f`.

[docs/appendix/DEPLOY.md](docs/appendix/DEPLOY.md) has the sizing table, equivalent machines on
GCP and Hetzner, and a script for shipping finished files to object storage.

## Kalshi

Kalshi needs an API key even for public market data. Create one at
[kalshi.com](https://kalshi.com), or at [demo.kalshi.co](https://demo.kalshi.co) for the demo
environment, which needs no funds. Save the private key as a `.pem` file and write a `.env` in the
repo root. Git ignores both `.env` and `*.pem`.

```bash
KALSHI_API_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi-key.pem
```

```bash
./demo.sh kalshi
```

```bash
KALSHI_DEMO=1 ./demo.sh kalshi
```

By hand, against production:

```bash
set -a; . ./.env; set +a
```

```bash
./target/release/collector --venue kalshi --max-markets 500 --duration-secs 120 > kalshi.sbe
```

For the demo environment, add
`--ws-url wss://demo-api.kalshi.co/trade-api/ws/v2 --rest-url https://demo-api.kalshi.co/trade-api/v2`.
Multivariate (MVE) markets are excluded. The live runs on the demo environment are in
[docs/KALSHI.md](docs/KALSHI.md).

Both venues at once, one process each, with their own output files:

```bash
./demo.sh both
```

## Checking an output file

```bash
./target/release/verify ob.sbe
```

It prints the message counts and the number of snapshots that matched the book rebuilt from the
updates before them. It ends with `OK`, or exits non-zero on any mismatch.
[docs/GRADING.md](docs/GRADING.md) explains each counter in the status lines.
