# tools/gen_arch_diagram.py
from pathlib import Path


def build_drawio_xml() -> str:
    def cell(i, v, x, y, w, h):
        return (
            f'<mxCell id="{i}" value="{v}" '
            'style="rounded=1;whiteSpace=wrap;html=1;align=center;verticalAlign=middle;strokeColor=#666;fillColor=#f5f5f5;" '
            'vertex="1" parent="1"><mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>'
        ).format(i=i, v=v, x=x, y=y, w=w, h=h)

    def edge(i, s, t):
        return (
            f'<mxCell id="{i}" edge="1" source="{s}" target="{t}" '
            'style="endArrow=block;endFill=1;rounded=0;html=1;strokeColor=#666;" parent="1">'
            '<mxGeometry relative="1" as="geometry"/></mxCell>'
        )

    cells = []
    # layout
    x0, y0, w, h, gx, gy = 40, 40, 250, 80, 300, 130
    # row 1
    cells += [
        cell("s3", "S3 raw dumps<br/>(EH export, HR scrapes)", x0, y0, w, h),
        cell("stage", "VS Code staging<br/>src/data/raw/", x0 + gx, y0, w, h),
        cell(
            "utils",
            "Python utils<br/>s3_utils.py • db_utils.py<br/>log_utils.py • view_utils.py",
            x0 + 2 * gx,
            y0,
            w,
            h,
        ),
    ]
    # row 2
    cells += [
        cell(
            "ingest",
            "ingest_peak_season.py<br/>Load CSV → base tables (Postgres)",
            x0,
            y0 + gy,
            w,
            h,
        ),
        cell(
            "view",
            "SQL VIEW<br/>player_peak_season_one_row<br/>(TOI-weighted totals; clean rates)",
            x0 + gx,
            y0 + gy,
            w,
            h,
        ),
        cell(
            "align",
            "build_player_streaks_and_aligned.py<br/>Five-year panels • rel_age −2…+2",
            x0 + 2 * gx,
            y0 + gy,
            w,
            h,
        ),
    ]
    # row 3
    cells += [
        cell(
            "z",
            "build_player_five_year_aligned_z.py<br/>Per-player z-scores + spicy + deltas",
            x0,
            y0 + 2 * gy,
            w,
            h,
        ),
        cell(
            "csv",
            "CSV snapshots<br/>hockeyref_final.csv<br/>eh_skater_seasons_2014_2025.csv<br/>player_five_year_panels.csv",
            x0 + gx,
            y0 + 2 * gy,
            w,
            h,
        ),
        cell(
            "nb",
            "Analysis notebooks<br/>figures & regression (file-first)",
            x0 + 2 * gx,
            y0 + 2 * gy,
            w,
            h,
        ),
    ]
    # arrows
    cells += [
        edge("e1", "s3", "stage"),
        edge("e2", "stage", "utils"),
        edge("e3", "utils", "ingest"),
        edge("e4", "ingest", "view"),
        edge("e5", "view", "align"),
        edge("e6", "align", "z"),
        edge("e7", "z", "csv"),
        edge("e8", "csv", "nb"),
    ]
    return f"""<mxfile host="app.diagrams.net">
  <diagram id="NHL-Beyond-27-arch" name="Architecture">
    <mxGraphModel grid="1" gridSize="10" guides="1" tooltips="1" connect="1" page="1" pageScale="1" pageWidth="1920" pageHeight="1080">
      <root><mxCell id="0"/><mxCell id="1" parent="0"/>{"".join(cells)}</root>
    </mxGraphModel>
  </diagram>
</mxfile>"""


if __name__ == "__main__":
    xml = build_drawio_xml()
    out_path = Path("docs/NHL-Beyond-27-architecture.drawio")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(xml, encoding="utf-8")
    print("Wrote", out_path.resolve())
