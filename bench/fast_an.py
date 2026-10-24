#!/usr/bin/env python3
"""Compare collectA.log (sources as before) and collectB.log (--fast-source) from fast.sh:
how far behind the first copy the written events were, per 5 minutes and overall."""
import re, sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
SB = re.compile(r"src_behind: n=(\d+) mean=(\S+) p50=(\S+) p90=(\S+) p99=(\S+)ms")
NUM = lambda k, l: int(m.group(1)) if (m := re.search(rf"\b{k}: (\d+)", l)) else 0

def load(path):
    rows, events = [], {"preferring fastest socket": 0, "socket stays slowest": 0}
    for line in open(path, errors="replace"):
        line = ANSI.sub("", line)
        for k in events:
            events[k] += k in line
        if " status " in line or line.rstrip().endswith('"status"') or "status" in line and "src_behind" in line:
            if (m := SB.search(line)):
                rows.append((line[11:19], int(m.group(1)), float(m.group(2)), m.group(3), m.group(4), m.group(5), line))
    return rows, events

for path in sys.argv[1:]:
    rows, events = load(path)
    t, n, mean, p50, p90, p99, last = rows[-1]
    print(f"== {path}: written events n={n} mean={mean} p50={p50} p90={p90} p99={p99} ms")
    print("   " + " ".join(f"{k}={NUM(k, last)}" for k in
          ("switches", "lag_switches", "disconnects", "recycles", "fast_picks", "fast_switches", "slow_recycles")),
          " log:", events)
    prev = (0, 0.0)
    for i, (t, n, mean, *_ ) in enumerate(rows):
        if i % 10 == 9 or i == len(rows) - 1:
            dn = n - prev[0]
            w = (mean * n - prev[1] * prev[0]) / dn if dn else float("nan")
            print(f"   until {t}: {dn:6d} events, mean behind {w:5.2f} ms")
            prev = (n, mean)
