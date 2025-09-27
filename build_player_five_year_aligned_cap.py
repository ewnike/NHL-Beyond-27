#!/usr/bin/env python3
# build_player_five_year_aligned_cap.py
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv  # pip install python-dotenv
from sqlalchemy import create_engine, text


def season_end_from_str(s: str) -> int | pd.NA:  # type: ignore
    """'13-14' -> 2014, '2013-14' -> 2014, '2014' -> 2014"""
    if s is None:
        return pd.NA
    s = str(s).strip().replace("–", "-").replace("—", "-").replace("/", "-")
    m = re.fullmatch(r"(\d{2})-(\d{2})", s)
    if m:
        _, y2 = map(int, m.groups())
        return 2000 + y2
    m = re.fullmatch(r"(\d{4})-(\d{2,4})", s)
    if m:
        y1, y2 = m.groups()
        y1 = int(y1)
        y2 = int(y2) if len(y2) == 4 else (y1 // 100) * 100 + int(y2)
        if y2 < y1:
            y2 += 100
        return y2
    if re.fullmatch(r"\d{4}", s):  # '2014'
        return int(s)
    if re.fullmatch(r"\d{2}", s):  # '14'
        return 2000 + int(s)
    return pd.NA


def normalize_name(s: str) -> str:
    return " ".join(str(s).split()).strip().lower()


def build_aligned_cap(
    *, engine, cap_dir: Path, table_in: str, table_out: str, out_csv: Path
) -> pd.DataFrame:
    # 1) read aligned table
    aligned = pd.read_sql(text(f"SELECT * FROM {table_in}"), engine)

    # pick player / season columns
    player_col = (
        "player"
        if "player" in aligned.columns
        else "player_name" if "player_name" in aligned.columns else "Player"
    )
    season_col = (
        "season"
        if "season" in aligned.columns
        else "season_end" if "season_end" in aligned.columns else None
    )
    if season_col is None:
        raise SystemExit("Aligned table needs 'season' or 'season_end'")

    aligned["_player_key"] = aligned[player_col].map(normalize_name)
    if season_col == "season_end":
        aligned["_season_end"] = pd.to_numeric(aligned[season_col], errors="coerce").astype("Int64")
    else:
        aligned["_season_end"] = pd.to_numeric(
            aligned[season_col].map(season_end_from_str), errors="coerce"
        ).astype("Int64")

    # 2) stack cap-hit CSVs (player_cap_hits_2014.csv, etc.)
    files = sorted(cap_dir.glob("player_cap_hits_*.csv"))
    if not files:
        raise SystemExit(f"No cap-hit CSVs found under {cap_dir}")

    caps = []
    for f in files:
        m = re.search(r"(\d{4})", f.stem)
        if not m:
            print(f"[WARN] skip (no YYYY in filename): {f}")
            continue
        yr = int(m.group(1))
        c = pd.read_csv(f, dtype=str, keep_default_na=False)

        # expect "player name" and "capHit"/"cap_hit"
        if "player name" not in c.columns:
            raise SystemExit(f"{f}: missing 'player name' column. Found: {list(c.columns)}")
        cap_col = (
            "capHit" if "capHit" in c.columns else ("cap_hit" if "cap_hit" in c.columns else None)
        )
        if cap_col is None:
            raise SystemExit(f"{f}: missing 'capHit' column. Found: {list(c.columns)}")

        c["_player_key"] = c["player name"].map(normalize_name)
        c["_season_end"] = yr
        c["_cap_hit"] = pd.to_numeric(
            c[cap_col].astype(str).str.replace(r"[^\d\.-]", "", regex=True), errors="coerce"
        )
        caps.append(c[["_player_key", "_season_end", "_cap_hit"]])

    cap = pd.concat(caps, ignore_index=True).drop_duplicates(
        ["_player_key", "_season_end"], keep="last"
    )

    # 3) merge
    merged = aligned.merge(cap, on=["_player_key", "_season_end"], how="left")
    merged["cap_hit_usd"] = merged["_cap_hit"].map(lambda v: f"${int(v):,}" if pd.notna(v) else "")
    merged = merged.drop(columns=["_player_key"])

    # 4) write outputs
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_csv, index=False)

    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_out}")
    merged.to_sql(table_out.split(".")[-1], engine, schema=table_out.split(".")[0], index=False)

    return merged


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Merge player_five_year_aligned with cap-hit CSVs and write a DB table + CSV."
    )
    ap.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="SQLAlchemy URL, e.g. postgresql+psycopg://user:pass@host:5432/dbname",
    )
    ap.add_argument("--table-in", default="public.player_five_year_aligned")
    ap.add_argument("--table-out", default="public.player_five_year_aligned_cap")
    ap.add_argument("--cap-dir", type=Path, default=Path("data/cap_hits"))
    ap.add_argument(
        "--out-csv", type=Path, default=Path("data/outputs/player_five_year_aligned_cap.csv")
    )
    return ap.parse_args()


def main():
    load_dotenv()  # load DATABASE_URL from .env into env vars
    args = parse_args()

    db_url = args.database_url or os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("Set --database-url or put DATABASE_URL in your .env")

    engine = create_engine(db_url)

    df = build_aligned_cap(
        engine=engine,
        cap_dir=args.cap_dir,
        table_in=args.table_in,
        table_out=args.table_out,
        out_csv=args.out_csv,
    )
    print(f"OK: wrote {args.table_out} and {args.out_csv} (rows={len(df)})")


if __name__ == "__main__":
    main()
