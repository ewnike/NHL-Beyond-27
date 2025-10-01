# tools/gen_arch_svg.py
from pathlib import Path


def build_svg() -> str:
    W, H = 1400, 800
    box_w, box_h = 310, 90
    gx, gy = 360, 160
    x0, y0 = 60, 60

    def wrap_lines(text: str, max_chars: int = 34) -> list[str]:
        """Greedy word-wrap by words; respects explicit \n breaks."""
        if not text:
            return []
        out: list[str] = []
        for raw in text.split("\n"):
            words = raw.split()
            if not words:
                out.append("")  # keep blank line
                continue
            line: list[str] = []
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

    def box(x, y, w, h, title, subtitle=None, id_=None, title_size=18, sub_size=13):
        # escape XML
        esc = lambda s: s.replace("&", "&amp;") if s else s

        # darker stroke/fill for pop
        rect = (
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" ry="12" '
            f'fill="#f0f0f0" stroke="#444" />'
        )

        # darker, bolder title
        t1 = (
            f'<text x="{x + w / 2}" y="{y + 30}" text-anchor="middle" '
            f'font-family="Inter,Arial" font-size="{title_size}" '
            f'font-weight="600" fill="#0b0b0b">{esc(title)}</text>'
        )

        # subtitle (optional): wrap to width and render as tspans
        t2 = ""
        if subtitle:
            max_chars = max(10, int(w // 9))  # ~9 px/char heuristic
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

        group = f'<g id="{id_}">{rect}{t1}{t2}</g>' if id_ else f"<g>{rect}{t1}{t2}</g>"
        return group

    def arrow(x1, y1, x2, y2):
        return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#666" stroke-width="2" marker-end="url(#arrow)"/>'

    coords = {
        "s3": (x0, y0),
        "stage": (x0 + gx, y0),
        "utils": (x0 + 2 * gx, y0),
        "ingest": (x0, y0 + gy),
        "view": (x0 + gx, y0 + gy),
        "align": (x0 + 2 * gx, y0 + gy),
        "z": (x0, y0 + 2 * gy),
        "csv": (x0 + gx, y0 + 2 * gy),
        "nb": (x0 + 2 * gx, y0 + 2 * gy),
    }

    def midbottom(x, y):
        return (x + box_w / 2, y + box_h)

    def midtop(x, y):
        return (x + box_w / 2, y)

    def midright(x, y):
        return (x + box_w, y + box_h / 2)

    def midleft(x, y):
        return (x, y + box_h / 2)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#666"/></marker></defs>',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        '<text x="50%" y="28" text-anchor="middle" font-family="Inter,Arial" font-size="22" fill="#111">NHL-Beyond-27 — Data Architecture</text>',
        box(*coords["s3"], box_w, box_h, "S3 raw dumps", "(EH export, HR scrapes)"),
        box(*coords["stage"], box_w, box_h, "VS Code staging", "src/data/raw/"),
        box(
            *coords["utils"],
            box_w,
            box_h,
            "Python utils",
            "s3_utils • db_utils • log_utils • view_utils",
        ),
        box(
            *coords["ingest"],
            box_w,
            box_h,
            "ingest_peak_season.py",
            "Load CSV → base tables (Postgres)",
        ),
        box(*coords["view"], box_w, box_h, "SQL VIEW", "player_peak_season_one_row"),
        box(
            *coords["align"],
            box_w,
            box_h,
            "build_player_streaks_and_aligned.py",
            "Five-year panels • rel_age −2…+2",
        ),
        box(
            *coords["z"],
            box_w,
            box_h,
            "build_player_five_year_aligned_z.py",
            "Per-player z-scores • spicy • deltas",
        ),
        box(
            *coords["csv"],
            box_w,
            box_h,
            "CSV snapshots",
            "hockeyref_final.csv • eh_skater_seasons_2014_2025.csv • player_five_year_panels.csv",
        ),
        box(*coords["nb"], box_w, box_h, "Analysis notebooks", "figures & regression (file-first)"),
        # arrows
        arrow(*midright(*coords["s3"]), *midleft(*coords["stage"])),
        arrow(*midright(*coords["stage"]), *midleft(*coords["utils"])),
        arrow(*midleft(coords["utils"][0] + 360, coords["utils"][1]), *midtop(*coords["ingest"])),
        arrow(*midright(*coords["ingest"]), *midleft(*coords["view"])),
        arrow(*midright(*coords["view"]), *midleft(*coords["align"])),
        arrow(*midleft(coords["align"][0] + 360, coords["align"][1]), *midtop(*coords["z"])),
        arrow(*midright(*coords["z"]), *midleft(*coords["csv"])),
        arrow(*midright(*coords["csv"]), *midleft(*coords["nb"])),
        "</svg>",
    ]
    return "\n".join(parts)


if __name__ == "__main__":
    svg = build_svg()
    out = Path("docs/NHL-Beyond-27-architecture.svg")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg, encoding="utf-8")
    print("Wrote", out.resolve())
