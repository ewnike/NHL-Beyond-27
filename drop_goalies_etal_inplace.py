"""
Final script that adds a few final touches to the
hockeyref_final.csv to make it compatible (apples to apples)
with evolution hockey download.

Eric Winiecke
September 28, 2025.
"""

#!/usr/bin/env python3
import logging
from pathlib import Path

import pandas as pd
from log_utils import setup_logger
from time_utils import compute_even_strength_minutes

logger = logging.getLogger(__name__)

FINAL = Path("data/outputs/hockeyref_final.csv")
GOALIES = Path("data/goalies/eh_goalies_study_years.csv")
GOALIE_COL = "Player"
ZERO_TOI_COL = "toi_seconds_total_ev"  # per-game EV seconds


def norm(s: str) -> str:
    return " ".join(str(s).split()).strip().lower()


def main():
    # Initialize logging if the caller hasn't already configured it
    if not logging.getLogger().handlers:
        setup_logger(None, level=logging.INFO)

    if not FINAL.exists():
        logger.error("Missing FINAL file: %s", FINAL)
        raise SystemExit(1)
    if not GOALIES.exists():
        logger.error("Missing GOALIES file: %s", GOALIES)
        raise SystemExit(1)

    df = pd.read_csv(FINAL, dtype=str, keep_default_na=False)

    # find the player-name column in the final
    for cand in ("player", "player_name", "name", "Player", "Name"):
        if cand in df.columns:
            player_col = cand
            break
    else:
        logger.error(
            "No player-name column found in %s (looked for player/player_name/name)",
            FINAL,
        )
        raise SystemExit(1)

    gdf = pd.read_csv(GOALIES, dtype=str, keep_default_na=False)
    if GOALIE_COL not in gdf.columns:
        logger.error(
            "Goalie column '%s' not in %s. Columns: %s",
            GOALIE_COL,
            GOALIES,
            list(gdf.columns),
        )
        raise SystemExit(1)

    gset = {norm(x) for x in gdf[GOALIE_COL].astype(str) if str(x).strip()}

    before = len(df)
    df["_np"] = df[player_col].astype(str).map(norm)
    mask_goalie = df["_np"].isin(gset)

    # zero/NaN per-game EV seconds mask
    if ZERO_TOI_COL in df.columns:
        toi_num = pd.to_numeric(df[ZERO_TOI_COL], errors="coerce")
        mask_zero_nan = toi_num.isna() | toi_num.eq(0)
    else:
        mask_zero_nan = pd.Series(False, index=df.index)
        logger.warning("Column '%s' not found; skipping zero/NaN-TOI drop.", ZERO_TOI_COL)

    # drop if goalie OR zero/NaN per-game EV seconds
    drop_mask = mask_goalie | mask_zero_nan

    removed_goalies = int(mask_goalie.sum())
    removed_zero_nan_only = int((mask_zero_nan & ~mask_goalie).sum())
    removed_total = int(drop_mask.sum())

    kept = df.loc[~drop_mask].drop(columns=["_np"])

    # compute season-total EV minutes (2-dec float)
    missing = [c for c in ("gp", ZERO_TOI_COL) if c not in kept.columns]
    if missing:
        logger.warning(
            "Cannot compute even-strength total minutes; missing columns: %s. "
            "File will be saved without 'toi_even_strength_min'.",
            missing,
        )
    else:
        kept = compute_even_strength_minutes(
            kept,
            seconds_per_game_col=ZERO_TOI_COL,
            gp_col="gp",
            out_minutes_col="toi_even_strength_min",
            out_minutes_str_col="toi_even_strength_min_str",
        )

    # drop cf_rel and common variants if present
    to_drop = [
        c for c in ("cf_rel", "cf_rel_ev", "cf_rel_std", "pos", "position") if c in kept.columns
    ]
    if to_drop:
        kept = kept.drop(columns=to_drop)
        logger.info("Dropped columns: %s", ", ".join(to_drop))

    kept.to_csv(FINAL, index=False)

    logger.info("Final rows before:                 %d", before)
    logger.info("Removed (goalie matches):          %d", removed_goalies)
    logger.info("Removed (zero/NaN %s): %d", ZERO_TOI_COL, removed_zero_nan_only)
    logger.info("Removed total:                     %d", removed_total)
    logger.info("Final rows after:                  %d", len(kept))

    if "toi_even_strength_min" in kept.columns:
        logger.info(
            "Sample EV minutes (2-dec): %s",
            kept["toi_even_strength_min"].head(3).tolist(),
        )


if __name__ == "__main__":
    main()
