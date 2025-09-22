from __future__ import annotations

import argparse
import re

import pandas as pd
from db_utils import (
    create_player_five_year_aligned_table,
    create_player_streak_seasons_table,
    get_db_engine,
    get_metadata,
)
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from view_utils import create_one_row_view

# ---------- helpers ----------
SEASON_RE = re.compile(r"^\d{2}-\d{2}$")
SOURCE_VIEW = "player_peak_season_one_row"  # <— use the view everywhere


def season_to_start_year(s: str) -> int | None:
    if not s or not SEASON_RE.match(s):
        return None
    yy = int(s[:2])
    return 1900 + yy if yy >= 50 else 2000 + yy


def ensure_player_peak_season_ready(conn, fq_table="public.player_peak_season"):
    # table exists?
    exists = conn.execute(
        text(
            """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = split_part(:t, '.', 1)
          AND table_name   = split_part(:t, '.', 2)
    """
        ),
        {"t": fq_table},
    ).scalar()
    if not exists:
        raise RuntimeError(f"{fq_table} does not exist. Create & load it first.")

    # required cols present?
    need = {"player", "season", "age", "CF%", "CF/60", "CA/60"}
    cols = {
        r[0]
        for r in conn.execute(
            text(
                """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = split_part(:t, '.', 1)
          AND table_name   = split_part(:t, '.', 2)
    """
            ),
            {"t": fq_table},
        ).all()
    }
    missing = need - cols
    if missing:
        raise RuntimeError(f"{fq_table} missing required columns: {sorted(missing)}")

    # non-empty?
    n = conn.execute(text(f"SELECT COUNT(*) FROM {fq_table}")).scalar_one()
    if n == 0:
        raise RuntimeError(f"{fq_table} is empty. Load data first.")

    # season format sanity
    bad = conn.execute(
        text(
            f"""
        SELECT COUNT(*) FROM {fq_table}
        WHERE season IS NULL OR season !~ '^[0-9]{{2}}-[0-9]{{2}}$'
    """
        )
    ).scalar_one()
    if bad:
        raise RuntimeError(f"{fq_table} has {bad} rows with invalid 'YY-YY' season strings.")


def streaks_from_years(years: list[int]) -> list[tuple[int, int, int]]:
    """Return list of (start_year, end_year, length) for maximal consecutive runs."""
    ys = sorted(set(years))
    if not ys:
        return []
    out = []
    run_s = prev = ys[0]
    for y in ys[1:]:
        if y == prev + 1:
            prev = y
        else:
            out.append((run_s, prev, prev - run_s + 1))
            run_s = prev = y
    out.append((run_s, prev, prev - run_s + 1))
    return out


def fetch_source_df(engine) -> pd.DataFrame:
    cols = [
        "player",
        "season",
        "age",
        "time_on_ice",  # minutes from the view
        "cf_pct",
        "cf60",
        "ca60",
        "position",
    ]
    sql = f"""
    SELECT {", ".join(cols)}
    FROM public.{SOURCE_VIEW}
    WHERE season ~ '^[0-9]{{2}}-[0-9]{{2}}$'
    """
    return pd.read_sql_query(sql, engine)


def main(
    streaks_table: str = "player_streak_seasons",
    aligned_table: str = "player_five_year_aligned",
    rebuild: bool = False,
    restrict_age_25_29: bool = False,
):
    engine = get_db_engine()
    md = get_metadata()

    # 1) Ensure/refresh the VIEW (fresh, aggregated one-row-per-player-season)
    create_one_row_view(engine)

    # 2) Prepare destination tables ONLY (do NOT create the view as a table)
    streaks_tbl = create_player_streak_seasons_table(streaks_table, md)
    aligned_tbl = create_player_five_year_aligned_table(aligned_table, md)
    md.create_all(engine, tables=[streaks_tbl, aligned_tbl])

    if rebuild:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"TRUNCATE TABLE public.{streaks_table}")
            conn.exec_driver_sql(f"TRUNCATE TABLE public.{aligned_table}")

    # 3) Pull clean source rows from the VIEW
    df = fetch_source_df(engine)
    if df.empty:
        print("No source rows found in view; aborting.")
        return

    # 4) Build per-player maps
    years_by_player: dict[str, list[int]] = {}
    metrics: dict[tuple[str, int], dict] = {}

    for r in df.itertuples(index=False):
        sy = season_to_start_year(r.season)
        if sy is None:
            continue
        p = r.player
        years_by_player.setdefault(p, []).append(sy)
        metrics[(p, sy)] = {
            "season": r.season,
            "age": r.age,
            "cf_pct": r.cf_pct,
            "cf60": r.cf60,
            "ca60": r.ca60,
            "toi_min": float(r.time_on_ice) if r.time_on_ice is not None else None,
            "position": getattr(r, "position", None),
        }

    # 5) Longest >=5 consecutive-season streak per player → upsert
    streak_rows = []
    for p, years in years_by_player.items():
        runs = streaks_from_years(years)
        if not runs:
            continue
        start, end, length = max(runs, key=lambda t: t[2])
        if length >= 5:
            seasons_txt = []
            for y in range(start, end + 1):
                m = metrics.get((p, y))
                seasons_txt.append(m["season"] if m else f"{str(y)[2:]}-{str(y + 1)[2:]}")
            streak_rows.append(
                {
                    "player": p,
                    "start_year": start,
                    "end_year": end,
                    "streak_len": length,
                    "seasons": seasons_txt,
                }
            )

    if streak_rows:
        with engine.begin() as conn:
            stmt = pg_insert(streaks_tbl).values(streak_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["player", "start_year", "end_year"],
                set_={
                    "streak_len": stmt.excluded.streak_len,
                    "seasons": stmt.excluded.seasons,
                    "created_at": text("now()"),
                },
            )
            conn.execute(stmt)

    # 6) Age-25..29 aligned window with avg TOI >= 500 (no peak-centering, no convex)
    aligned_rows = []
    for p, years in years_by_player.items():
        # map each age in 25..29 -> earliest season year + metrics
        by_age = {}
        for y in sorted(set(years)):
            m = metrics.get((p, y))
            if not m:
                continue
            a = m.get("age")
            if a is None:
                continue
            if 25 <= a <= 29 and a not in by_age:
                by_age[a] = (y, m)

        # must have all five ages
        if not all(a in by_age for a in (25, 26, 27, 28, 29)):
            continue

        # avg TOI across the five ages must be >= 500 minutes
        toi_list = [by_age[a][1].get("toi_min") for a in (25, 26, 27, 28, 29)]
        if any(t is None or t <= 0 for t in toi_list):
            continue
        if sum(toi_list) / 5.0 < 500.0:
            continue

        # anchor at age 27 so rel_age = -2..-1..0..1..2
        anchor_year = by_age[27][0]

        for age in (25, 26, 27, 28, 29):
            start_year, m = by_age[age]
            rel = age - 27
            aligned_rows.append(
                {
                    "player": p,
                    "peak_year": anchor_year,  # reused as "anchor (age-27) year"
                    "rel_age": rel,  # -2,-1,0,1,2
                    "start_year": start_year,
                    "season": m["season"],
                    "age": age,
                    "cf_pct": m["cf_pct"],
                    "cf60": m["cf60"],
                    "ca60": m["ca60"],
                    "position": m["position"],
                    # "time_on_ice": m["toi_min"],  # add if your aligned table has this column
                }
            )

    # 💾 write once after building the full batch
    if aligned_rows:
        with engine.begin() as conn:
            # ensure the column exists (safe even if it already does)
            conn.exec_driver_sql(
                "ALTER TABLE public.player_five_year_aligned ADD COLUMN IF NOT EXISTS position text"
            )

            stmt = pg_insert(aligned_tbl).values(aligned_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["player", "peak_year", "rel_age"],
                set_={
                    "start_year": stmt.excluded.start_year,
                    "season": stmt.excluded.season,
                    "age": stmt.excluded.age,
                    "cf_pct": stmt.excluded.cf_pct,
                    "cf60": stmt.excluded.cf60,
                    "ca60": stmt.excluded.ca60,
                    "position": stmt.excluded.position,
                    "created_at": text("now()"),
                },
            )
            conn.execute(stmt)

    print(
        f"streak rows upserted: {len(streak_rows)}  |  "
        f"aligned rows upserted: {len(aligned_rows)}  "
        f"({len(aligned_rows) // 5} players)"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build player streaks and five-year aligned windows.")
    ap.add_argument(
        "--rebuild",
        action="store_true",
        help="Truncate destination tables before inserting (full replace).",
    )
    ap.add_argument(
        "--restrict-age-25-29",
        action="store_true",
        help="Keep only aligned windows where all five ages are within [25,29].",
    )
    args = ap.parse_args()
    main(rebuild=args.rebuild, restrict_age_25_29=args.restrict_age_25_29)
