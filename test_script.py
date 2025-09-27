import os
import re
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

# -------- project paths --------
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
SEASONS_DIR = DATA_DIR / "seasons"  # S3 downloads land here
PEAK_CSV = DATA_DIR / "peak_player_season_stats.csv"
HR_STACK_CSV = DATA_DIR / "hockeyref_stacked.csv"  # saved stacked HR
QA_DIR = DATA_DIR / "qa"  # small reports
QA_HR_ONLY = QA_DIR / "hr_only_keys.csv"
QA_PEAK_ONLY = QA_DIR / "peak_only_keys.csv"
# --------------------------------

# filenames like nhl_player_seasons_2014.csv
S3_FILE_REGEX = r"(?i)nhl_player_seasons_\d{4}\.csv$"


def yyyy_to_season(yyyy) -> str:
    try:
        y = int(str(yyyy)[:4])
    except Exception:
        return str(yyyy)
    return f"{(y - 1) % 100:02d}-{y % 100:02d}"


def season_from_start_end(start, end) -> str:
    try:
        s = int(str(start)[:4])
        e = int(str(end)[:4])
        return f"{s % 100:02d}-{e % 100:02d}"
    except Exception:
        return None


def normalize_peak_columns(peak: pd.DataFrame) -> pd.DataFrame:
    # 1) lowercase + snake-ish for matching (keep original names too)
    orig_cols = peak.columns.tolist()
    norm_map = {c: re.sub(r"[^A-Za-z0-9]+", "_", c).strip("_").lower() for c in orig_cols}
    peak = peak.rename(columns=norm_map)

    # 2) build 'player'
    if "player" not in peak.columns:
        if "player_name" in peak.columns:
            peak = peak.rename(columns={"player_name": "player"})
        elif "name" in peak.columns:
            peak = peak.rename(columns={"name": "player"})

    # 3) build 'season'
    if "season" not in peak.columns:
        # single year
        if "year" in peak.columns:
            peak["season"] = peak["year"].apply(yyyy_to_season)
        # start/end year pattern
        elif {"start_year", "end_year"}.issubset(peak.columns):
            peak["season"] = [
                season_from_start_end(s, e)
                for s, e in zip(peak["start_year"], peak["end_year"], strict=False)
            ]
        # already like "2013-14" but under another name
        elif "season_text" in peak.columns:
            peak = peak.rename(columns={"season_text": "season"})
        elif "seasonid" in peak.columns:  # e.g., 20142015

            def from_seasonid(x):
                s = str(x)
                if len(s) == 8:
                    return f"{int(s[:4]) % 100:02d}-{int(s[4:]) % 100:02d}"
                return s

            peak["season"] = peak["seasonid"].apply(from_seasonid)

    # 4) tidy
    if "player" in peak.columns:
        peak["player"] = peak["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # 5) sanity log (optional)
    # print("Peak normalized columns:", list(peak.columns))

    return peak


def clean_hockeyref(df: pd.DataFrame) -> pd.DataFrame:
    # normalize headers
    df = df.rename(
        columns={c: re.sub(r"[^A-Za-z0-9]+", "_", c).strip("_").lower() for c in df.columns}
    )
    # make 'player'
    if {"first_name", "last_name"}.issubset(df.columns) and "player" not in df.columns:
        df["player"] = (df["first_name"].fillna("") + " " + df["last_name"].fillna("")).str.strip()
    if "player" not in df.columns and "player_name" in df.columns:
        df = df.rename(columns={"player_name": "player"})
    # season: YYYY -> "YY-YY"
    if "season" in df.columns and df["season"].astype(str).str.fullmatch(r"\d{4}").any():
        df["season"] = df["season"].apply(yyyy_to_season)
    elif "year" in df.columns:
        df["season"] = df["year"].apply(yyyy_to_season)
    # numerics
    for c in ("age", "gp", "g", "a", "pts"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # tidy player
    if "player" in df.columns:
        df["player"] = df["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    # keep a stable minimal set for comparison; extra cols are fine, they’ll be saved too
    for c in ("player", "season"):
        if c not in df.columns:
            df[c] = pd.NA
    return df


def list_keys(bucket, prefix, pattern=S3_FILE_REGEX):
    s3 = boto3.client("s3")
    patt = re.compile(pattern)
    keys, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if patt.search(Path(k).name):
                keys.append(k)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def download(bucket, keys, dest: Path):
    s3 = boto3.client("s3")
    dest.mkdir(parents=True, exist_ok=True)
    paths = []
    for k in keys:
        p = dest / Path(k).name
        s3.download_file(bucket, k, str(p))
        paths.append(p)
    return paths


if __name__ == "__main__":
    load_dotenv()

    # resolve env
    bucket = os.getenv("S3_BUCKET_NAME", "ewnike-mads593-nhl")
    prefix = os.getenv("S3_PREFIX", "nhl_player_seasons_data/")
    explicit = [k.strip() for k in os.getenv("S3_KEYS", "").split(",") if k.strip()] or None

    # ensure dirs
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SEASONS_DIR.mkdir(parents=True, exist_ok=True)
    QA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) get season files from S3 -> ./data/seasons
    if explicit:
        paths = download(bucket, explicit, SEASONS_DIR)
    else:
        keys = list_keys(bucket, prefix)
        if not keys:
            raise SystemExit(
                f"No season CSVs under s3://{bucket}/{prefix} matching {S3_FILE_REGEX}"
            )
        paths = download(bucket, keys, SEASONS_DIR)

    # 2) clean + stack HR
    hr_frames = [clean_hockeyref(pd.read_csv(p)) for p in paths]
    hr = pd.concat(hr_frames, ignore_index=True)
    hr.to_csv(HR_STACK_CSV, index=False)  # save the stacked HR dataset
    print(f"[OK] Hockey-Ref stacked → {HR_STACK_CSV}  shape={hr.shape}")

    # 3) load peak CSV
    # if not PEAK_CSV.exists():
    #     raise SystemExit(f"Peak CSV not found at: {PEAK_CSV.resolve()}")
    # peak = pd.read_csv(PEAK_CSV)
    # # normalize basic join fields
    # if "player" not in peak.columns and "player_name" in peak.columns:
    #     peak = peak.rename(columns={"player_name": "player"})
    # if "season" not in peak.columns and "year" in peak.columns:
    #     peak["season"] = peak["year"].apply(yyyy_to_season)
    # if "player" in peak.columns:
    #     peak["player"] = peak["player"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    peak = pd.read_csv(PEAK_CSV)
    peak = normalize_peak_columns(peak)

    # sanity check / quick debug
    print("Peak columns after normalize:", list(peak.columns)[:12])

    required = {"player", "season"}
    if not required.issubset(peak.columns):
        raise SystemExit(
            f"Peak CSV is missing required columns after normalization. Have: {list(peak.columns)}"
        )

    # now this will work
    peak_keys = peak[["player", "season"]].dropna().drop_duplicates()

    # 4) compare sizes + keys
    print(f"[INFO] HR rows       : {len(hr):,}")
    print(f"[INFO] Peak rows     : {len(peak):,}")

    hr_keys = hr[["player", "season"]].dropna().drop_duplicates()
    peak_keys = peak[["player", "season"]].dropna().drop_duplicates()
    print(f"[INFO] HR unique (player,season)   : {len(hr_keys):,}")
    print(f"[INFO] Peak unique (player,season) : {len(peak_keys):,}")

    # find mismatches
    hr_only = hr_keys.merge(peak_keys, on=["player", "season"], how="left", indicator=True)
    hr_only = hr_only[hr_only["_merge"] == "left_only"].drop(columns=["_merge"])
    peak_only = peak_keys.merge(hr_keys, on=["player", "season"], how="left", indicator=True)
    peak_only = peak_only[peak_only["_merge"] == "left_only"].drop(columns=["_merge"])

    hr_only.to_csv(QA_HR_ONLY, index=False)
    peak_only.to_csv(QA_PEAK_ONLY, index=False)

    print(f"[QA] Keys in HR only   : {len(hr_only):,}  → {QA_HR_ONLY}")
    print(f"[QA] Keys in Peak only : {len(peak_only):,}  → {QA_PEAK_ONLY}")

    print(
        "\nNext step: when counts look right, we can do the actual merge to a DataFrame identical to the peak table."
    )
