# # Recreate the .drawio file. The previous state was reset; rebuild everything in one cell.

from xml.sax.saxutils import escape

cells = []


def add_cell(
    id_,
    value,
    x,
    y,
    w,
    h,
    style="rounded=1;whiteSpace=wrap;html=1;align=center;verticalAlign=middle;strokeColor=#666;fillColor=#f5f5f5;",
):
    cells.append(f'''
      <mxCell id="{id_}" value="{escape(value)}" style="{style}" vertex="1" parent="1">
        <mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/>
      </mxCell>
    ''')


def add_edge(
    id_, src, dst, points=None, style="endArrow=block;endFill=1;rounded=0;html=1;strokeColor=#666;"
):
    points_tag = ""
    if points:
        pts = "".join([f'<mxPoint x="{px}" y="{py}" />' for (px, py) in points])
        points_tag = f'<Array as="points">{pts}</Array>'
    cells.append(f'''
      <mxCell id="{id_}" edge="1" source="{src}" target="{dst}" style="{style}" parent="1">
        <mxGeometry relative="1" as="geometry">
          {points_tag}
        </mxGeometry>
      </mxCell>
    ''')


# Layout
x0, y0 = 40, 40
w, h = 250, 80
gapx, gapy = 300, 130

# Row 1
add_cell("s3", "S3 raw dumps<br/>(EH export, HR scrapes)", x0, y0, w, h)
add_cell("staging", "VS Code local staging<br/>src/data/raw/", x0 + gapx, y0, w, h)
add_cell(
    "utils",
    "Python utils<br/>s3_utils.py • db_utils.py<br/>log_utils.py • view_utils.py",
    x0 + 2 * gapx,
    y0,
    w,
    h,
)

# Row 2
add_cell(
    "ingest", "ingest_peak_season.py<br/>Load CSV → base tables (Postgres)", x0, y0 + gapy, w, h
)
add_cell(
    "view",
    "SQL VIEW<br/>player_peak_season_one_row<br/>(TOI-weighted totals; clean rates)",
    x0 + gapx,
    y0 + gapy,
    w,
    h,
)
add_cell(
    "align",
    "build_player_streaks_and_aligned.py<br/>Five-year panels • rel_age −2…+2",
    x0 + 2 * gapx,
    y0 + gapy,
    w,
    h,
)

# Row 3
add_cell(
    "z",
    "build_player_five_year_aligned_z.py<br/>Per-player z-scores + spicy + deltas",
    x0,
    y0 + 2 * gapy,
    w,
    h,
)
add_cell(
    "csv",
    "CSV snapshots<br/>hockeyref_final.csv<br/>eh_skater_seasons_2014_2025.csv<br/>player_five_year_panels.csv",
    x0 + gapx,
    y0 + 2 * gapy,
    w,
    h,
)
add_cell(
    "nb",
    "Analysis notebooks<br/>figures & regression (file-first)",
    x0 + 2 * gapx,
    y0 + 2 * gapy,
    w,
    h,
)

# Edges
add_edge("e1", "s3", "staging")
add_edge("e2", "staging", "utils")
add_edge("e3", "utils", "ingest")
add_edge("e4", "ingest", "view")
add_edge("e5", "view", "align")
add_edge("e6", "align", "z")
add_edge("e7", "z", "csv")
add_edge("e8", "csv", "nb")

xml = f"""<mxfile host="app.diagrams.net">
  <diagram id="NHL-Beyond-27-arch" name="Architecture">
    <mxGraphModel dx="1162" dy="827" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1920" pageHeight="1080" math="0" shadow="0">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>
        {"".join(cells)}
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>"""

out_path = "/mnt/data/NHL-Beyond-27-architecture.drawio"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(xml)

print(out_path)
