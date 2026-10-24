#!/usr/bin/env python3
"""Summarise collect.log from lat.sh: final per-socket/per-edge lines, and whether the
per-socket ranking by mean `behind` holds between the first and second half of the run."""
import re, sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
KV = re.compile(r"(\w+)=(n=\d+ mean=\S+ p50=\S+ p90=\S+ p99=\S+ms|\S+)")

def hist(v):
    d = dict(x.split("=") for x in v.replace("ms", "").split())
    return {k: float(x) for k, x in d.items()}

reports = []  # list of (time, {conn: fields}, {edge: fields})
cur = None
for line in open(sys.argv[1], errors="replace"):
    line = ANSI.sub("", line)
    if "socket latency" not in line and "edge latency" not in line:
        continue
    t = line[:19]
    f = dict(KV.findall(line))
    for k in ("wire", "behind"):
        f[k] = hist(f[k])
    if cur is None or ("socket latency" in line and cur[2]):
        cur = (t, {}, {})
        reports.append(cur)
    if "socket latency" in line:
        cur[1][f["conn"]] = f
    else:
        cur[2][f["edge"]] = f

def second_half(a, b):
    """Mean of the samples between report a and b, from the cumulative n and mean."""
    n = b["n"] - a["n"]
    return (b["mean"] * b["n"] - a["mean"] * a["n"]) / n if n else float("nan")

final = reports[-1]
mid = min(reports, key=lambda r: abs(len(reports) / 2 - reports.index(r)))
print(f"reports={len(reports)} mid={mid[0]} final={final[0]}")
print("conn       rep edge             first  late   wire_mean p50 p99 | behind mean p50 p90 p99 | h1 behind  h2 behind  h2-h1 wire")
for c, f in sorted(final[1].items(), key=lambda kv: kv[1]["behind"]["mean"]):
    m = mid[1].get(c)
    h1 = m["behind"]["mean"] if m else float("nan")
    h2 = second_half(m["behind"], f["behind"]) if m else float("nan")
    w1 = m["wire"]["mean"] if m else float("nan")
    w2 = second_half(m["wire"], f["wire"]) if m else float("nan")
    w, b = f["wire"], f["behind"]
    print(f"{c:>10} {f['replica']:>3} {f['edge']:<16} {f['first']:>6} {f['late']:>5}   {w['mean']:7.1f} {w['p50']:4.0f} {w['p99']:4.0f} | "
          f"{b['mean']:6.2f} {b['p50']:3.0f} {b['p90']:3.0f} {b['p99']:4.0f} | {h1:8.2f} {h2:9.2f}  {w2 - w1:+7.1f}")
for e, f in final[2].items():
    print(f"edge {e:<16} sockets={f['sockets']} first={f['first']} late={f['late']} wire={f['wire']} behind={f['behind']}")
