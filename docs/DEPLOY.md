# Running it in the cloud

All full-universe runs in this repo used a GitHub Codespace: Azure WestEurope, 4 vCPU, 16 GB.
This note says what the collector needs from a host and gives the closest machine on the
common providers. Only the Codespace has been run. The other rows match it on paper.

## What the collector needs

The numbers are measured in the 2.5-hour soak on the full universe (ENGINEERING-LOG).

| Resource | Measured | Plan for |
|---|---|---|
| CPU | 172–197% of 4 vCPU (about 2 cores) | 4 vCPU. Two core threads, plus decode on the tokio runtime |
| Memory | RSS 2.8–3.3 GB, rising with the universe | 8 GB minimum, 16 GB to match the tested host |
| Disk write | about 5 GB per 10 minutes: 8.5 MB/s sustained, 30 GB an hour | a disk that sustains ≥ 50 MB/s. The Codespace disk stalled the writer for 1–1.5 s eight times in 2.5 h |
| Disk space | 30 GB an hour of output | rotate (`--rotate-mins`) and ship finished files off the host |
| Network | about 450 websockets, outbound only | no inbound ports. `ulimit -n` ≥ 4096 |
| Clock | Codespace drifted about 0.45 ms a minute | chrony against the provider's time service, so `wire` is absolute |

## Equivalent machines

| Provider | Machine | vCPU / RAM | Disk for output | Time sync |
|---|---|---|---|---|
| GitHub Codespaces (tested) | 4-core | 4 / 16 GB | the workspace disk (slow, see above) | none |
| AWS | `m7i.xlarge` (or `c7i.xlarge` with 8 GB) | 4 / 16 GB | gp3 EBS (125 MB/s baseline) or an instance-store NVMe type (`m7gd`, `c7gd`) | Amazon Time Sync, `169.254.169.123`, via chrony |
| GCP | `n2-standard-4` (or `c3-standard-4`) | 4 / 16 GB | `pd-balanced`, or Hyperdisk Balanced for more throughput | the metadata server, `metadata.google.internal`, via chrony |
| Hetzner | `CCX23` (dedicated vCPU) | 4 / 16 GB | local NVMe | chrony against the Hetzner NTP servers |

A few notes on choosing:
- **CPU:** dedicated or non-burstable vCPU matters more than clock speed. The load is steady,
  around 2 cores, and a burstable type (AWS `t3`, GCP `e2-medium`) would run out of credits.
- **Region:** pick the one with the lowest `wire` to Polymarket. Measure it rather than guess:
  run `--lat-detail` for a few minutes in each candidate region and compare `behind` and `wire`
  (LATENCY explains both).
- **ARM** (AWS Graviton, GCP Tau T2A, Hetzner CAX) should work, since nothing is x86-specific.
  It has not been built or run there.

## Setup on a fresh Linux host

```bash
sudo apt-get update && sudo apt-get install -y build-essential chrony git
```

```bash
curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
```

```bash
git clone <repo> collector && cd collector && cargo build --release --bins
```

Point chrony at the provider's time source:
- AWS: `server 169.254.169.123 prefer iburst`;
- GCP: `server metadata.google.internal prefer iburst`.

Then check it with `chronyc tracking`.

## Run it as a service

`--supervise` restarts the collector within seconds of a crash. It also recovers the
crashed period's `.part` file, keeping every whole chunk (D20). systemd then only has to keep
the supervisor itself alive:

```ini
# /etc/systemd/system/collector.service
[Unit]
Description=Polymarket order-book collector
After=network-online.target chrony.service
Wants=network-online.target

[Service]
WorkingDirectory=/opt/collector
ExecStart=/opt/collector/target/release/collector --supervise --out /data/ob.sbe --rotate-mins 10 --universe-cache /data/universe.json
KillSignal=SIGINT
TimeoutStopSec=60
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

- `KillSignal=SIGINT` gives the orderly shutdown: close the sockets, write the last chunk,
  close the file.
- `TimeoutStopSec` leaves time for the last 64 MiB chunk to be written.
- Logs go to the journal (`journalctl -u collector`).

## Shipping the output

A file is written as `<name>.sbe.part` and renamed to `<name>.sbe` only once every core has
moved past its period. So any file ending in `.sbe` is finished, and each one replays on its
own. A cron job or timer can verify each finished file and move it to object storage:

```bash
for f in /data/ob.*.sbe; do ./target/release/verify "$f" > "$f.verify" && aws s3 mv "$f" s3://<bucket>/; done
```

Use `gcloud storage mv` on GCP. On Hetzner, use `rclone move` to a Storage Box or to Object
Storage. `verify` exits non-zero on any mismatch, so a bad file stays on disk for inspection.

## Not done

- The collector has run only on the Codespace. The AWS, GCP and Hetzner rows are the same shape
  of machine, and none of them has been run.
- No container image. A static binary plus systemd is all the service needs. A Dockerfile would
  add the `ulimit` and time sync as things to get right inside the container.
