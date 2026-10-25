#!/usr/bin/env python3
"""Draws the README's line charts from data.json as standalone SVG files.

Standard library only. Each chart is one measure on one axis; x is the run, labelled with the
fixes that landed before it. Colours follow the OS light/dark setting inside each SVG.

    python3 docs/charts/make_charts.py
"""
import json
import math
from html import escape
from pathlib import Path

HERE = Path(__file__).parent

# Validated categorical slots 1-2 (dataviz validator: CVD dE 24.7 light / 26.8 dark). Lines take
# .sN (stroke); markers take .fN (fill) and .ring, a 2px ring in the surface colour.
STYLE = """
.bg{fill:#fcfcfb}.t1{fill:#0b0b0b}.t2{fill:#52514e}.t3{fill:#7a7974}.grid{stroke:#e6e5e0}
.axis{stroke:#c9c8c2}.s0{stroke:#2a78d6}.s1{stroke:#eb6834}.f0{fill:#2a78d6}.f1{fill:#eb6834}
.ring{stroke:#fcfcfb}.wash0{fill:#2a78d6}
@media (prefers-color-scheme: dark){
.bg{fill:#1a1a19}.t1{fill:#f0efec}.t2{fill:#c3c2b7}.t3{fill:#94938b}.grid{stroke:#2e2e2c}
.axis{stroke:#46453f}.s0{stroke:#3987e5}.s1{stroke:#d95926}.f0{fill:#3987e5}.f1{fill:#d95926}
.ring{stroke:#1a1a19}.wash0{fill:#3987e5}}
text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
"""


def fmt(v, unit):
    if unit == "ms":
        if v >= 1000:
            return f"{v / 1000:g} s"
        if v < 1:
            return f"{v * 1000:g} µs"
        return f"{v:g} ms"
    if unit == "B":
        return f"{v:,.0f} B"
    if unit == "%":
        return f"{v:g}%"
    if unit == "GB":
        return f"{v:g} GB"
    if unit in ("k", "k/s"):
        return f"{v:g}k"
    return f"{v:g}"


def nice_ticks(lo, hi, n=4):
    span = hi - lo or 1
    raw = span / n
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    start = math.floor(lo / step) * step
    ticks, t = [], start
    while t <= hi + step * 0.001:
        ticks.append(round(t, 10))
        t += step
    if ticks[-1] < hi:
        ticks.append(round(ticks[-1] + step, 10))
    return ticks


def chart(c):
    small = c.get("small", False)
    W = 380 if small else 760
    H = 270 if small else 300
    left, right, top, bottom = (56, 18, 62, 52) if small else (64, 28, 62, 52)
    # Wrap the subtitle to the chart width; a second line pushes the plot down.
    maxch = int((W - 32) / 6.4)
    lines, cur = [], ""
    for w in c["subtitle"].split():
        if cur and len(cur) + len(w) + 1 > maxch:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    lines = lines[:2]
    top += 13 * (len(lines) - 1)
    H += 13 * (len(lines) - 1)
    pw, ph = W - left - right, H - top - bottom
    log = c.get("log", False)
    unit = c["unit"]
    xs = c["x"]
    vals = [v for s in c["series"] for v in s["points"]]
    vals += [v for s in c["series"] for v in (s.get("hi") or []) if v is not None]
    vals += [v for s in c["series"] for v in (s.get("lo") or []) if v is not None]

    if log:
        lo_e = math.floor(math.log10(min(vals)))
        hi_e = math.ceil(math.log10(max(vals)))
        ticks = [10.0 ** e for e in range(lo_e, hi_e + 1)]
        def y(v):
            return top + ph - (math.log10(v) - lo_e) / (hi_e - lo_e) * ph
    else:
        ticks = nice_ticks(0, max(vals))
        tmax = ticks[-1]
        def y(v):
            return top + ph - v / tmax * ph

    n = len(xs)
    step = pw / n
    def x(i):
        return left + step * (i + 0.5)

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-labelledby="t d">',
           f'<title id="t">{escape(c["title"])}</title><desc id="d">{escape(c["subtitle"])}</desc>',
           f"<style>{STYLE}</style>",
           f'<rect class="bg" width="{W}" height="{H}" rx="8"/>',
           f'<text class="t1" x="16" y="26" font-size="15" font-weight="600">{escape(c["title"])}</text>']
    tx = 16
    for k, ln in enumerate(lines):
        out.append(f'<text class="t2" x="{tx}" y="{44 + 13 * k}" font-size="11.5">{escape(ln)}</text>')

    for t in ticks:
        yy = y(t)
        out.append(f'<line class="grid" x1="{left}" x2="{W - right}" y1="{yy:.1f}" y2="{yy:.1f}" stroke-width="1"/>')
        out.append(f'<text class="t3" x="{left - 8}" y="{yy + 4:.1f}" font-size="11" text-anchor="end">{escape(fmt(t, unit))}</text>')
    out.append(f'<line class="axis" x1="{left}" x2="{W - right}" y1="{top + ph}" y2="{top + ph}" stroke-width="1"/>')

    for i, p in enumerate(xs):
        out.append(f'<text class="t2" x="{x(i):.1f}" y="{top + ph + 17}" font-size="11" text-anchor="middle" font-weight="600">{escape(p["run"])}</text>')
        out.append(f'<text class="t3" x="{x(i):.1f}" y="{top + ph + 31}" font-size="10" text-anchor="middle">{escape(p["fix"])}</text>')

    for si, s in enumerate(c["series"]):
        pts = [(x(i), y(v)) for i, v in enumerate(s["points"])]
        if len(c["series"]) == 1 and not log:
            area = f"M{pts[0][0]:.1f},{top + ph} " + " ".join(f"L{a:.1f},{b:.1f}" for a, b in pts) + f" L{pts[-1][0]:.1f},{top + ph} Z"
            out.append(f'<path class="wash0" d="{area}" opacity="0.10"/>')
        for i, (lo, hi) in enumerate(zip(s.get("lo") or [None] * n, s.get("hi") or [None] * n)):
            if lo is not None and hi is not None:
                out.append(f'<line class="s{si}" x1="{x(i):.1f}" x2="{x(i):.1f}" y1="{y(lo):.1f}" y2="{y(hi):.1f}" stroke-width="1.5" opacity="0.6"/>')
        d = " ".join(("M" if i == 0 else "L") + f"{a:.1f},{b:.1f}" for i, (a, b) in enumerate(pts))
        out.append(f'<path class="s{si}" d="{d}" fill="none" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
        for i, (a, b) in enumerate(pts):
            v = s["points"][i]
            out.append(f'<circle class="f{si} ring" cx="{a:.1f}" cy="{b:.1f}" r="4.5" stroke-width="2"><title>{escape(xs[i]["run"])}: {escape(fmt(v, unit))}</title></circle>')
        # Direct labels: first, last and the extremes only.
        vs = s["points"]
        pick = {0, n - 1, vs.index(max(vs)), vs.index(min(vs))}
        for i in sorted(pick):
            a, b = pts[i]
            above = si == 0 or len(c["series"]) == 1
            if len(c["series"]) > 1:
                other = c["series"][1 - si]["points"][i]
                above = vs[i] >= other
            dy = -10 if above else 18
            # Clear a whisker: anchor the label at the whisker's end, not the point.
            his, los = s.get("hi") or [None] * n, s.get("lo") or [None] * n
            if above and his[i] is not None:
                b = y(his[i])
            elif not above and los[i] is not None:
                b = y(los[i])
            if above and b - top < 14:
                dy = 18
            if not above and top + ph - b < 22:
                dy = -10
            out.append(f'<text class="t1" x="{a:.1f}" y="{b + dy:.1f}" font-size="11" text-anchor="middle" font-weight="600">{escape(fmt(vs[i], unit))}</text>')

    if len(c["series"]) > 1:
        lx = W - right
        for si, s in reversed(list(enumerate(c["series"]))):
            tw = len(s["name"]) * 6.3 + 22
            lx -= tw
            out.append(f'<line class="s{si}" x1="{lx:.1f}" x2="{lx + 12:.1f}" y1="22" y2="22" stroke-width="2"/>')
            out.append(f'<text class="t2" x="{lx + 16:.1f}" y="26" font-size="11">{escape(s["name"])}</text>')
    out.append("</svg>")
    return "\n".join(out)


def main():
    data = json.loads((HERE / "data.json").read_text())
    for c in data["charts"]:
        path = HERE / f"{c['id']}.svg"
        path.write_text(chart(c) + "\n")
        print("wrote", path.relative_to(HERE.parent.parent))


if __name__ == "__main__":
    main()
