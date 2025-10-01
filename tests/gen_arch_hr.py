# Re-run the SVG generation (the previous cell was reset).

from pathlib import Path

W, H = 1500, 920
box_w, box_h = 330, 96
gx, gy = 380, 170
x0, y0 = 60, 60


def wrap_lines(text: str, max_chars: int = 34) -> list[str]:
    if not text:
        return []
    out = []
    for raw in text.split("\n"):
        words = raw.split()
        if not words:
            out.append("")
            continue
        line = []
        for w in words:
            cand = (" ".join(line + [w])).strip()
            if len(cand) <= max_chars:
                line.append(w)
            else:
                out.append(" ".join(line))
                line = [w]
        if line:
            out.append(" ".join(line))
    return out


def box(
    x, y, w, h, title, subtitle=None, title_size=18, sub_size=13, stroke="#444", fill="#f0f0f0"
):
    esc = lambda s: s.replace("&", "&amp;") if s else s
    rect = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" ry="12" fill="{fill}" stroke="{stroke}" />'
    t1 = (
        f'<text x="{x + w / 2}" y="{y + 30}" text-anchor="middle" '
        f'font-family="Inter,Arial" font-size="{title_size}" '
        f'font-weight="600" fill="#0b0b0b">{esc(title)}</text>'
    )
    t2 = ""
    if subtitle:
        max_chars = max(10, int(w // 9))
        lines = wrap_lines(subtitle, max_chars=max_chars)
        y0 = y + 52
        tspans = []
        for i, line in enumerate(lines):
            dy = 0 if i == 0 else 15
            tspans.append(f'<tspan x="{x + w / 2}" dy="{dy}">{esc(line)}</tspan>')
        t2 = (
            f'<text x="{x + w / 2}" y="{y0}" text-anchor="middle" '
            f'font-family="Inter,Arial" font-size="{sub_size}" '
            f'font-weight="500" fill="#1a1a1a">{"".join(tspans)}</text>'
        )
    return f"<g>{rect}{t1}{t2}</g>"


def arrow(x1, y1, x2, y2, stroke="#444"):
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
        f'stroke="{stroke}" stroke-width="2" marker-end="url(#arrow)"/>'
    )


coords = {
    "scr_players": (x0, y0),
    "scr_even": (x0 + gx, y0),
    "scr_goalies": (x0 + 2 * gx, y0),
    "raw_reg": (x0, y0 + gy),
    "raw_even": (x0 + gx, y0 + gy),
    "raw_goal": (x0 + 2 * gx, y0 + gy),
    "dl": (x0 + gx, y0 + 2 * gy),
    "build": (x0, y0 + 3 * gy),
    "drop_g": (x0 + gx, y0 + 3 * gy),
    "diff": (x0 + 2 * gx, y0 + 3 * gy),
    "final": (x0 + gx, y0 + 4 * gy),
}


def midbottom(x, y):
    return (x + box_w / 2, y + box_h)


def midtop(x, y):
    return (x + box_w / 2, y)


def midright(x, y):
    return (x + box_w, y + box_h / 2)


def midleft(x, y):
    return (x, y + box_h / 2)


svg = [
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
    '<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#444"/></marker></defs>',
    '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
    '<text x="50%" y="32" text-anchor="middle" font-family="Inter,Arial" font-size="24" fill="#0b0b0b" font-weight="700">NHL-Beyond-27 — Hockey-Reference Pipeline</text>',
]

# Row 1: scripts
svg.append(
    box(
        *coords["scr_players"],
        box_w,
        box_h,
        "scrap_hockey_ref_player.py",
        "Regular-season skater pages\n(also goalie pages)",
    )
)
svg.append(
    box(
        *coords["scr_even"],
        box_w,
        box_h,
        "scrap_hcky_ref_evenstrength.py",
        "Even-strength tables (all skaters)",
    )
)
svg.append(
    box(
        *coords["scr_goalies"],
        box_w,
        box_h,
        "Goalie scrape (via player pages)",
        "Goalie-specific rows",
    )
)

# Row 2: raw data targets
svg.append(box(*coords["raw_reg"], box_w, box_h, "Regular season (all players)", "Per-season CSVs"))
svg.append(box(*coords["raw_even"], box_w, box_h, "Even strength (all players)", "Per-season CSVs"))
svg.append(box(*coords["raw_goal"], box_w, box_h, "Goalies", "Per-season CSVs"))

# Row 3: download / staging
svg.append(
    box(
        *coords["dl"],
        box_w,
        box_h,
        "Data download / staging",
        "data/seasons/*.csv • data/even_strength/*.csv",
    )
)

# Row 4: processing
svg.append(
    box(
        *coords["build"],
        box_w,
        box_h,
        "build_ref_hockey.py",
        "Concat + normalize + merge\n(TOT rows, TOI totals, weighted CF%)",
    )
)
svg.append(
    box(
        *coords["drop_g"],
        box_w,
        box_h,
        "drop_goalies_etal_inplace.py",
        "Filter out goalies & misc.\nAnalysis set: F/D only",
    )
)
svg.append(
    box(
        *coords["diff"],
        box_w,
        box_h,
        "diff_players_by_season.py",
        "Diagnostics: keyset diffs\n(player, season)",
    )
)

# Row 5: final
svg.append(
    box(*coords["final"], box_w, box_h, "Output", "data/outputs/hockeyref_final.csv", title_size=19)
)

# Arrows
svg.append(arrow(*midbottom(*coords["scr_players"]), *midtop(*coords["raw_reg"])))  # scripts -> raw
svg.append(arrow(*midbottom(*coords["scr_even"]), *midtop(*coords["raw_even"])))
svg.append(arrow(*midbottom(*coords["scr_goalies"]), *midtop(*coords["raw_goal"])))

svg.append(arrow(*midbottom(*coords["raw_reg"]), *midtop(*coords["dl"])))  # raw -> dl
svg.append(arrow(*midbottom(*coords["raw_even"]), *midtop(*coords["dl"])))
svg.append(arrow(*midbottom(*coords["raw_goal"]), *midtop(*coords["dl"])))

svg.append(arrow(*midleft(*coords["dl"]), *midright(*coords["build"])))  # dl -> processing
svg.append(arrow(*midright(*coords["build"]), *midleft(*coords["drop_g"])))
svg.append(arrow(*midright(*coords["drop_g"]), *midleft(*coords["diff"])))

svg.append(arrow(*midbottom(*coords["drop_g"]), *midtop(*coords["final"])))  # processing -> final

svg.append("</svg>")
svg_text = "\n".join(svg)

out = Path("docs/NHL-Beyond-27-hockeyref-pipeline.svg")
out.write_text(svg_text, encoding="utf-8")

str(out)
