# # #!/usr/bin/env python3
# import argparse
# import re
# import unicodedata
# from pathlib import Path

# import numpy as np
# import pandas as pd


# def _norm_name(s: str) -> str:
#     s = str(s).strip().lower()
#     s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
#     s = re.sub(r"[^a-z0-9]+", " ", s)
#     return " ".join(s.split())


# def _find_player_col(df: pd.DataFrame) -> str | None:
#     cands = ["player", "player_name", "name"]
#     lowmap = {c.lower(): c for c in df.columns}
#     for c in cands:
#         if c in lowmap:
#             return lowmap[c]
#     for c in df.columns:
#         if "player" in c.lower():
#             return c
#     return None


# def season_end_from_str(s: str):
#     if s is None:
#         return np.nan
#     s = str(s).strip().replace("–", "-").replace("—", "-").replace("/", "-")

#     m = re.fullmatch(r"(\d{4})-(\d{4})", s)
#     if m:
#         y1, y2 = int(m.group(1)), int(m.group(2))
#         if y2 < y1:
#             y2 += 100
#         return y2
#     m = re.fullmatch(r"(\d{4})-(\d{2})", s)
#     if m:
#         y1, y2 = int(m.group(1)), int(m.group(2))
#         y2 = (y1 // 100) * 100 + y2
#         if y2 < y1:
#             y2 += 100
#         return y2
#     m = re.fullmatch(r"(\d{2})-(\d{4})", s)
#     if m:
#         a, y2 = int(m.group(1)), int(m.group(2))
#         y1 = (y2 // 100) * 100 + a
#         if y1 > y2:
#             y1 -= 100
#         return y2
#     m = re.fullmatch(r"(\d{2})-(\d{2})", s)
#     if m:
#         a, b = int(m.group(1)), int(m.group(2))
#         century = 1900 if a >= 90 else 2000
#         y1 = century + a
#         y2 = century + b
#         if y2 < y1:
#             y2 += 100
#         return y2
#     if re.fullmatch(r"\d{4}", s):
#         return int(s)
#     return np.nan


# def _ensure_season_end(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.copy()
#     cols = {c.lower(): c for c in df.columns}
#     if "season_end" in cols:
#         df["season_end"] = pd.to_numeric(df[cols["season_end"]], errors="coerce").astype("Int64")
#         return df
#     if "season" in cols:
#         df["season_end"] = df[cols["season"]].map(season_end_from_str).astype("Int64")
#         return df
#     df["season_end"] = pd.Series([pd.NA] * len(df), dtype="Int64")
#     return df


# def load_frame(path: Path) -> pd.DataFrame:
#     df = pd.read_csv(path)
#     df.columns = [str(c).strip() for c in df.columns]
#     df = _ensure_season_end(df)
#     pcol = _find_player_col(df)
#     if not pcol:
#         raise SystemExit(f"No player-like column in {path}. Columns: {list(df.columns)}")
#     df["_player_key"] = df[pcol].map(_norm_name)
#     return df


# def main():
#     ap = argparse.ArgumentParser(description="Diff players between two CSVs.")
#     ap.add_argument("--left", required=True, help="Left CSV (e.g., HR filtered)")
#     ap.add_argument("--right", required=True, help="Right CSV (aligned set)")
#     ap.add_argument("--outdir", default="data/outputs", help="Where to write diff CSVs")
#     args = ap.parse_args()

#     left = Path(args.left)
#     right = Path(args.right)
#     outdir = Path(args.outdir)
#     if not left.exists():
#         raise SystemExit(f"Left CSV not found: {left}")
#     if not right.exists():
#         raise SystemExit(f"Right CSV not found: {right}")
#     outdir.mkdir(parents=True, exist_ok=True)

#     L = load_frame(left)
#     R = load_frame(right)

#     # Name-only diff
#     # lnames = set(L["_player_key"].dropna().unique())
#     # rnames = set(R["_player_key"].dropna().unique())
#     # only_left_names = sorted(lnames - rnames)
#     # only_right_names = sorted(rnames - lnames)

#     # pd.DataFrame({"player_norm": only_left_names}).to_csv(
#     #     outdir / "players_only_left.csv", index=False
#     # )
#     # pd.DataFrame({"player_norm": only_right_names}).to_csv(
#     #     outdir / "players_only_right.csv", index=False
#     # )
#     # ---------- Name-only diff (print real names + write one CSV) ----------
#     def _canonical_name_map(df: pd.DataFrame) -> dict[str, str]:
#         """For each _player_key, pick the most common display name in that frame."""
#         pcol = _find_player_col(df)  # already exists from load_frame path
#         # Use mode; if tie, pick first
#         return (
#             df[[pcol, "_player_key"]]
#             .dropna(subset=["_player_key"])
#             .groupby("_player_key")[pcol]
#             .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0])
#             .to_dict()
#         )

#     # Build the normalized key sets
#     lnames = set(L["_player_key"].dropna().unique())
#     rnames = set(R["_player_key"].dropna().unique())
#     only_left_keys = sorted(lnames - rnames)
#     only_right_keys = sorted(rnames - lnames)

#     # Map back to nice display names (prefer the side they came from)
#     left_name_map = _canonical_name_map(L)
#     right_name_map = _canonical_name_map(R)
#     only_left_names_disp = [left_name_map.get(k, k) for k in only_left_keys]
#     only_right_names_disp = [right_name_map.get(k, k) for k in only_right_keys]

#     # Print to stdout (your “32 and 26” lists)
#     print("\n=== NAME DIFFS ===")
#     print(f"Left-only ({len(only_left_names_disp)}):")
#     for n in only_left_names_disp:
#         print("  -", n)
#     print(f"\nRight-only ({len(only_right_names_disp)}):")
#     for n in only_right_names_disp:
#         print("  -", n)
#     print()

#     # Also write a single CSV summarizing both sides
#     name_rows = [
#         {"side": "left_only", "player_norm": k, "display_name": n}
#         for k, n in zip(only_left_keys, only_left_names_disp, strict=False)
#     ] + [
#         {"side": "right_only", "player_norm": k, "display_name": n}
#         for k, n in zip(only_right_keys, only_right_names_disp, strict=False)
#     ]
#     names_out = outdir / "players_name_diffs.csv"
#     pd.DataFrame(name_rows).to_csv(names_out, index=False)
#     print(f"Name diffs written to: {names_out}")

#     # Pair-level diff (guard if season_end missing)
#     can_pairs = L["season_end"].notna().any() and R["season_end"].notna().any()
#     if can_pairs:
#         Lp = L.dropna(subset=["season_end"]).copy()
#         Rp = R.dropna(subset=["season_end"]).copy()
#         Lp["_season_end_int"] = Lp["season_end"].astype(int)
#         Rp["_season_end_int"] = Rp["season_end"].astype(int)

#         left_pairs = set(zip(Lp["_player_key"], Lp["_season_end_int"], strict=False))
#         right_pairs = set(zip(Rp["_player_key"], Rp["_season_end_int"], strict=False))
#         only_left_pairs = sorted(left_pairs - right_pairs)
#         only_right_pairs = sorted(right_pairs - left_pairs)

#         pd.DataFrame(only_left_pairs, columns=["player_norm", "season_end"]).to_csv(
#             outdir / "pairs_only_left.csv", index=False
#         )
#         pd.DataFrame(only_right_pairs, columns=["player_norm", "season_end"]).to_csv(
#             outdir / "pairs_only_right.csv", index=False
#         )
#     else:
#         print("Note: could not perform (player, season_end) pair diff — season_end missing.")

#     # Summary to stdout
#     # === DIFF SUMMARY ===
#     print("=== DIFF SUMMARY ===")
#     print(f"Left : {left}")
#     print(f"Right: {right}")
#     print(
#         f"Unique players → left={len(lnames)} | right={len(rnames)} | "
#         f"left-only={len(only_left_names_disp)} | right-only={len(only_right_names_disp)}"
#     )

#     if can_pairs:
#         print(
#             f"Pair CSVs written to: {outdir}/pairs_only_left.csv and {outdir}/pairs_only_right.csv"
#         )
#     print(
#         f"Name CSVs written to: {outdir}/players_only_left.csv and {outdir}/players_only_right.csv"
#     )


# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd

# ---------- Normalization helpers ----------


def strip_accents(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_name(s: str) -> str:
    """
    Lowercase, strip accents, remove parentheticals (e.g., ' (D)'),
    remove punctuation, collapse whitespace, merge leading initials,
    then apply first-name alias mapping.
    """
    if s is None:
        return ""
    s = strip_accents(s).lower().strip()

    # remove parentheticals e.g. "erik gustafsson (d)" -> "erik gustafsson"
    s = re.sub(r"\([^)]*\)", " ", s)

    # treat dotted initials like "t.j." -> "tj" (first remove dots/spacers)
    s = s.replace(".", " ")

    # remove any non-alphanumeric (keep spaces)
    s = re.sub(r"[^a-z0-9]+", " ", s)

    # collapse spaces
    s = " ".join(s.split())

    # merge *leading* two single-letter initials: "t j brodie" -> "tj brodie"
    s = re.sub(r"^([a-z])\s+([a-z])\b", r"\1\2", s)

    # apply first-name alias mapping (e.g., mat/matt/mathew -> matthew)
    s = apply_alias(s)

    return s


# Common alias mappings to converge obvious nickname/full-name variants
ALIAS_MAP = {
    # collapse to canonical first names
    "mat": "matthew",
    "matt": "matthew",
    "mathew": "matthew",
    "alex": "alexander",
    "pat": "patrick",
    "ben": "benjamin",
    "chris": "christopher",
    "dan": "daniel",
    "dave": "david",
    "jake": "jacob",
    "jon": "jonathon",
    "joe": "joseph",
    "josh": "joshua",
    "mike": "michael",
    "nick": "nicholas",
    "nate": "nathan",
    "sam": "samuel",
    "tom": "thomas",
    # initials like “T.J.” → “tj”
    "tj": "tj",  # canonical target
    "t j": "tj",  # handled below, but keep for safety
}


def apply_alias(norm_key: str) -> str:
    toks = norm_key.split()
    if not toks:
        return norm_key
    first = toks[0]
    if first in ALIAS_MAP:
        toks[0] = ALIAS_MAP[first]
    return " ".join(toks)


# ---------- I/O + diff ----------


def find_player_col(df: pd.DataFrame) -> str:
    # Try common names
    candidates = ["player", "player_name", "name"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    # Fallback: any column containing 'player'
    for c in df.columns:
        if "player" in c.lower():
            return c
    # If all else fails, just pick the first column
    return df.columns[0]


def load_names(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    col = find_player_col(df)
    out = df[[col]].rename(columns={col: "player"})
    return out


def main():
    ap = argparse.ArgumentParser(description="Diff player names with normalization.")
    ap.add_argument("--left", required=True, help="Left CSV (e.g., hr_filtered_sorted_relage.csv)")
    ap.add_argument("--right", required=True, help="Right CSV (e.g., player_five_year_aligned.csv)")
    ap.add_argument("--outdir", default="data/outputs", help="Output directory for CSVs")
    args = ap.parse_args()

    left_path = Path(args.left)
    right_path = Path(args.right)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    L = load_names(left_path)
    R = load_names(right_path)

    # Build normalized keys
    for df in (L, R):
        df["player_norm"] = df["player"].map(normalize_name).map(apply_alias)
    # Drop empties after normalization
    L = L[L["player_norm"] != ""].drop_duplicates("player_norm").reset_index(drop=True)
    R = R[R["player_norm"] != ""].drop_duplicates("player_norm").reset_index(drop=True)

    left_set = set(L["player_norm"])
    right_set = set(R["player_norm"])

    # These are the *true* diffs after normalization
    only_left_norm = sorted(left_set - right_set)
    only_right_norm = sorted(right_set - left_set)

    # For transparency, show which original spellings mapped to matched normalized names
    # (i.e., disagreements resolved by normalization)
    matched_norm = sorted(left_set & right_set)

    # Build “resolved matches” table to see original spellings on both sides
    if matched_norm:
        L_res = L[L["player_norm"].isin(matched_norm)].copy()
        R_res = R[R["player_norm"].isin(matched_norm)].copy()
        # Aggregate originals per norm key for both sides
        L_grp = L_res.groupby("player_norm")["player"].apply(lambda s: sorted(set(s))).reset_index()
        R_grp = R_res.groupby("player_norm")["player"].apply(lambda s: sorted(set(s))).reset_index()
        resolved = L_grp.merge(
            R_grp, on="player_norm", suffixes=("_left_variants", "_right_variants")
        )
    else:
        resolved = pd.DataFrame(
            columns=["player_norm", "player_left_variants", "player_right_variants"]
        )

    # Build final name-only diff frames with original spellings
    only_left_df = L[L["player_norm"].isin(only_left_norm)][["player", "player_norm"]].sort_values(
        "player_norm"
    )
    only_right_df = R[R["player_norm"].isin(only_right_norm)][
        ["player", "player_norm"]
    ].sort_values("player_norm")

    # Write outputs
    left_csv = outdir / "players_only_left_normalized.csv"
    right_csv = outdir / "players_only_right_normalized.csv"
    res_csv = outdir / "normalized_matches_resolved.csv"

    only_left_df.to_csv(left_csv, index=False)
    only_right_df.to_csv(right_csv, index=False)
    resolved.to_csv(res_csv, index=False)

    # Console summary
    print("=== NORMALIZED NAME DIFFS ===")
    print(f"Left : {left_path}")
    print(f"Right: {right_path}")
    print(f"Unique players → left={len(left_set)} | right={len(right_set)}")
    print(
        f"After normalization → left-only={len(only_left_norm)} | right-only={len(only_right_norm)}"
    )
    print()
    if only_left_norm:
        print("Left-only (up to 30):")
        print("  - " + "\n  - ".join(only_left_df["player"].head(30)))
    if only_right_norm:
        print("\nRight-only (up to 30):")
        print("  - " + "\n  - ".join(only_right_df["player"].head(30)))
    print(f"\nName CSVs written to:\n  {left_csv}\n  {right_csv}")
    print(f"Resolved-by-normalization report:\n  {res_csv}")


if __name__ == "__main__":
    main()
