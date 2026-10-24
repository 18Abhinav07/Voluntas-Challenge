# The AWS run

One run, on AWS credits, that produces every number the write-ups quote from a production-like
host: absolute latency from three regions, full-universe completeness, CPU and memory per thread
group, disk and network rates, crash recovery, the offline benchmarks, and a sample capture for
`sample/`. The kit is `bench/aws/run.sh` (on the laptop) and `bench/aws/host.sh` (on each host).

## Hosts

| Role | Region | Type | Why |
|---|---|---|---|
| main | eu-west-2 (London) | c7i.xlarge: 4 vCPU, 8 GB, about $0.21/h | Closest region to the Cloudflare edge that served the earlier probes; 4 vCPU is the size DEPLOY.md recommends |
| probe | eu-west-1 (Ireland) | c7i-flex.large: 2 vCPU, about $0.10/h | Second European region |
| probe | us-east-1 (N. Virginia) | c7i-flex.large | US East, where most crypto venues sit |

Expected cost: under $2 (main about 2 h, probes about 40 min each, 100 GB gp3 for 2 h; inbound
traffic is free and the results are a few MB).

## Phases

| # | Phase | Hosts | Duration | Gate |
|---|---|---|---|---|
| 0 | Preflight: identity, vCPU quota per region, `run-instances --dry-run` per type (catches the Free-plan type limit), Ubuntu AMI, default VPC | laptop | 1 min | Any failure stops before anything is launched |
| 1 | Setup: packages, chrony on Amazon Time Sync (and the ENA PTP clock if exposed), build on main only, binaries copied to probes; host facts and pings to the edges | all | about 10 min | `SETUP_OK` |
| 2 | Smoke: 45 s on 20 markets, then `verify` | all | 1 min | **GO/NO-GO**: main must pass or the run stops (geo-block, Cloudflare, DNS) |
| 3 | Region probe: 2,000 markets, `--lat-detail --kernel-ts`, started at the same second on every host | all | 10 min | 0 replay mismatches; probes terminated right after |
| 4 | `cargo test --release`, `bench cpu`, `bench mem 283000` | main | 3 min | recorded |
| 5 | Full universe, 60 min: `--supervise --pin --kernel-ts --rotate-mins 10`. Sampled every 10 s: CPU per thread group, RSS, network in, disk out; chrony every 5 min; a 30 s `perf` profile at 30 min; `kill -9` of the child at 55 min (the supervisor must recover). Each finished file is verified (niced) and deleted. | main | 60 min | 0 mismatches in every file |
| 6 | Sample: 2 min on 500 markets, kept with its `verify` report | main | 2 min | `SAMPLE_OK` |

## Safety

- Every resource is tagged `Project=voluntas-run`. `run.sh all` tears everything down on any
  exit (the data is pulled first). `run.sh teardown` does the same by hand.
- Every instance halts itself, and so terminates, after a fixed lifetime (main 240 min, probes
  75 min), even if the laptop sleeps or loses the network.
- ssh is open only to the laptop's IP, with a key made for the run (kept in
  `~/.cache/voluntas-run`, never in the repo). No access keys are created.
- Each host writes its results and a marker per phase under `~/results`, so a dropped ssh session
  loses nothing; the laptop pulls every 10 minutes.
- `host.sh` was dry-run end to end on a Codespace (`DRY=1`, short durations) before the real run.

## Running it

```bash
aws login
```

```bash
bench/aws/run.sh preflight
```

```bash
caffeinate -i bench/aws/run.sh all
```

Results land in `bench/aws-<date>/`: `driver.log`, and per host `summary.txt`, `env.txt`,
`res-*.txt`, `verify-*.txt`, `collect-*.log`, `bench.txt`, `tests.log`, `perf-top.txt` and
`sample/`.
