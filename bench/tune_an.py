#!/usr/bin/env python3
"""Tables for the AWS tuning rounds (bench/aws/host.sh tune): one row per collector run.

    python3 bench/tune_an.py bench/aws-tune-DATE/main-eu-west-2

Each row is the run's last status line (counters and histograms are totals since start), the
median of its per-window host-delay p50/p99, its CPU and RSS means, and its verify result.
"""
import re
import statistics
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
MS = lambda k: re.compile(rf"{k}: n=(\d+) mean=(\S+) p50=(\S+) p90=(\S+) p99=(\S+)ms")
HIST = lambda k: re.compile(rf"\b{k}: n=(\d+) p50<=(\S+)ms p99<=(\S+)ms")
HOST = re.compile(r"host delay .*p50<=(\d+) p90<=\d+ p99<=(\d+)")
NUM = lambda k, l: int(m.group(1)) if (m := re.search(rf"\b{k}: (\d+)", l)) else 0
COUNTS = ("updates", "switches", "lag_switches", "switch_snaps", "resnapshots", "disconnects",
          "fast_picks", "fast_switches", "fast_stalls", "slow_recycles", "fast_disagree")


def run(log, res, ver):
    last, host = "", []
    for line in open(log, errors="replace"):
        line = ANSI.sub("", line)
        if "collector: status" in line:
            last = line
        elif (m := HOST.search(line)):
            host.append((int(m.group(1)), int(m.group(2))))
    row = {k: NUM(k, last) for k in COUNTS}
    for k in ("src_wire", "src_behind"):
        m = MS(k).search(last)
        row[k] = f"{m.group(2)} / {m.group(3)} / {m.group(5)}" if m else "-"
    m = HIST("wire").search(last)
    row["wire"] = f"{m.group(2)} / {m.group(3)}" if m else "-"
    if host:
        row["host"] = f"{statistics.median(h[0] for h in host):g} / {statistics.median(h[1] for h in host):g}"
    if res.exists():
        cols = {}
        for l in res.read_text().splitlines():
            f = l.split()
            if int(f[0]) <= 60:
                continue
            for kv in f[1:]:
                k, _, v = kv.partition("=")
                try:
                    cols.setdefault(k, []).append(float(v))
                except ValueError:
                    pass
        row["cpu"] = " ".join(f"{k}={statistics.mean(v):.0f}" for k, v in sorted(cols.items()) if k in ("core", "tokio", "main", "writer"))
        row["rss"] = f"{statistics.mean(cols.get('rss_mb', [0])):.0f}"
    if ver.exists():
        t = ver.read_text()
        bad = sum(int(x) for x in re.findall(r"(\d+) did not", t))
        row["verify"] = "OK" if " 0 did not" in t and bad == 0 else f"FAIL ({bad} did not)"
    return row


def main(d):
    d = Path(d)
    for log in sorted(d.glob("collect-r*.log")):
        name = log.stem.removeprefix("collect-")
        r = run(log, d / f"res-{name}.txt", d / f"verify-{name}.txt")
        print(f"== {name}")
        print("   written events, recv-exch ms mean/p50/p99:", r["src_wire"], "| behind first copy:", r["src_behind"])
        print("   all copies wire p50/p99:", r["wire"], "| host delay us p50/p99 (median window):", r.get("host", "-"))
        print("  ", " ".join(f"{k}={r[k]}" for k in COUNTS))
        print("   cpu %:", r.get("cpu", "-"), "rss MB:", r.get("rss", "-"), "verify:", r.get("verify", "-"))


if __name__ == "__main__":
    for a in sys.argv[1:]:
        main(a)
