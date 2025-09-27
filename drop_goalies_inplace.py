# drop_goalies_inplace.py
from pathlib import Path

import pandas as pd

FINAL = Path("data/outputs/hockeyref_final.csv")
GOALIES = Path("data/goalies/eh_goalies_study_years.csv")  # adjust path if different
GOALIE_COL = "Player"  # <-- your column name
ZERO_TOI_COL = "toi_seconds_total_ev"


def norm(s: str) -> str:
    return " ".join(str(s).split()).strip().lower()


def main():
    df = pd.read_csv(FINAL, dtype=str, keep_default_na=False)

    # find the player-name column in the final
    for cand in ("player", "player_name", "name", "Player", "Name"):
        if cand in df.columns:
            player_col = cand
            break
    else:
        raise SystemExit(
            f"No player-name column found in {FINAL} (looked for player/player_name/name)"
        )

    gdf = pd.read_csv(GOALIES, dtype=str, keep_default_na=False)
    if GOALIE_COL not in gdf.columns:
        raise SystemExit(
            f"Goalie column '{GOALIE_COL}' not in {GOALIES}. Columns: {list(gdf.columns)}"
        )

    gset = {norm(x) for x in gdf[GOALIE_COL].astype(str) if str(x).strip()}

    before = len(df)
    df["_np"] = df[player_col].astype(str).map(norm)
    # kept = df[~df["_np"].isin(gset)].drop(columns=["_np"])
    mask_goalie = df["_np"].isin(gset)

    # zero/NaN TOI mask
    if ZERO_TOI_COL in df.columns:
        toi_num = pd.to_numeric(df[ZERO_TOI_COL], errors="coerce")
        mask_zero_nan = toi_num.isna() | toi_num.eq(0)
    else:
        mask_zero_nan = pd.Series(False, index=df.index)
        print(f"[WARN] Column '{ZERO_TOI_COL}' not found; skipping zero/NaN-TOI drop.")

    # combine: drop if goalie OR zero/NaN
    drop_mask = mask_goalie | mask_zero_nan

    removed_goalies = int(mask_goalie.sum())
    removed_zero_nan_only = int((mask_zero_nan & ~mask_goalie).sum())
    removed_total = int(drop_mask.sum())

    kept = df.loc[~drop_mask].drop(columns=["_np"])
    kept.to_csv(FINAL, index=False)

    print(f"Final rows before:                 {before}")
    print(f"Removed (goalie matches):          {removed_goalies}")
    print(f"Removed (zero/NaN {ZERO_TOI_COL}): {removed_zero_nan_only}")
    print(f"Removed total:                     {removed_total}")
    print(f"Final rows after:                  {len(kept)}")


if __name__ == "__main__":
    main()
