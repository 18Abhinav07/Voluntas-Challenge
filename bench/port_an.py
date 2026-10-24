#!/usr/bin/env python3
"""Summarise bench/port.sh: per round, each replica slot's mean `behind` (ms behind the first
copy of the same event on any socket) from the round's last `socket latency` line. Then asks
whether a slot's speed carries over to the next round: for fixed-port slots the 4-tuple is the
same every round, for ephemeral slots it is new. Usage: port_an.py FIXED collect-1.log ..."""
import re, statistics as st, sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
KV = re.compile(r"(\w+)=(n=\d+ mean=\S+ p50=\S+ p90=\S+ p99=\S+ms|\S+)")

def hist(v):
    return {k: float(x) for k, x in (p.split("=") for p in v.replace("ms", "").split())}

def last_report(path):
    rep = {}
    for line in open(path, errors="replace"):
        line = ANSI.sub("", line)
        if "socket latency" in line:
            f = dict(KV.findall(line))
            rep[int(f["replica"])] = {"behind": hist(f["behind"]), "wire": hist(f["wire"])}
    return rep

def ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0] * len(xs)
    for k, i in enumerate(order):
        r[i] = k
    return r

def spearman(a, b):
    ra, rb = ranks(a), ranks(b)
    n = len(a)
    return 1 - 6 * sum((x - y) ** 2 for x, y in zip(ra, rb)) / (n * (n * n - 1))

def icc(rows):
    """Share of variance between slots (ICC(1)) from a slots x rounds table."""
    k = len(rows[0])
    grand = st.mean(x for r in rows for x in r)
    msb = k * sum((st.mean(r) - grand) ** 2 for r in rows) / (len(rows) - 1)
    msw = sum((x - st.mean(r)) ** 2 for r in rows for x in r) / (len(rows) * (k - 1))
    return (msb - msw) / (msb + (k - 1) * msw) if msb + (k - 1) * msw else float("nan")

fixed = int(sys.argv[1])
rounds = [last_report(p) for p in sorted(sys.argv[2:], key=lambda p: int(re.findall(r"\d+", p)[-1]))]
slots = sorted(set.intersection(*(set(r) for r in rounds)))
mean = {s: [r[s]["behind"]["mean"] for r in rounds] for s in slots}
print("behind mean (ms) per slot and round; F = fixed source port, E = ephemeral")
print("slot      " + " ".join(f"r{k + 1:<5}" for k in range(len(rounds))) + "  mean  sd")
for s in slots:
    v = mean[s]
    print(f"{s:>3} {'F' if s < fixed else 'E'}    " + " ".join(f"{x:6.2f}" for x in v) + f"  {st.mean(v):5.2f} {st.pstdev(v):4.2f}")
print("p99 behind per slot and round")
for s in slots:
    print(f"{s:>3} {'F' if s < fixed else 'E'}    " + " ".join(f"{r[s]['behind']['p99']:6.0f}" for r in rounds))
for name, group in (("fixed", [s for s in slots if s < fixed]), ("ephemeral", [s for s in slots if s >= fixed])):
    if len(group) < 3:
        continue
    rhos = [spearman([mean[s][k] for s in group], [mean[s][k + 1] for s in group]) for k in range(len(rounds) - 1)]
    print(f"{name:>9}: round-to-round rank correlation {' '.join(f'{x:+.2f}' for x in rhos)} "
          f"(mean {st.mean(rhos):+.2f}); ICC {icc([mean[s] for s in group]):+.2f}")
spread = [max(r[s]["behind"]["mean"] for s in slots) - min(r[s]["behind"]["mean"] for s in slots) for r in rounds]
print("spread (slowest - fastest mean) per round: " + " ".join(f"{x:.2f}" for x in spread))
