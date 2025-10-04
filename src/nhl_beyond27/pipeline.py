from __future__ import annotations

import logging
import sys
import subprocess
import shutil
from importlib import import_module
from importlib import util as importlib_util
from pathlib import Path

log = logging.getLogger(__name__)

# repo root = .../NHL-Beyond-27
ROOT = Path(__file__).resolve().parents[2]  # unify on ROOT everywhere


# ----------------------------
# Dump to S3 (wrapper for scripts/dump_to_s3.sh)
# ----------------------------
def dump_db_to_s3(dbname: str, s3_uri: str, *, sse: str = "AES256") -> None:
    """
    Run the repo's dump script:
      dumps -> compresses -> sha256 -> uploads to S3 -> writes manifest.
    Requirements: pg_dump, zstd, aws CLI available; PG* env vars set if needed.
    """
    script = ROOT / "scripts" / "dump_to_s3.sh"
    if not script.exists():
        raise FileNotFoundError(f"Missing dump script: {script}")

    for tool in ("pg_dump", "zstd", "aws"):
        if not shutil.which(tool):
            raise RuntimeError(f"Required tool not found on PATH: {tool}")

    cmd = [str(script), dbname, s3_uri, sse]
    subprocess.run(cmd, check=True)


# ----------------------------
# Dynamic import helpers (your existing logic)
# ----------------------------
def load_optional(module_name: str, filename: str, required: bool = False):
    try:
        mod = __import__(module_name, fromlist=["*"])
        return mod
    except Exception as err:  # capture the original exception
        msg = f"Missing module/file: {module_name} ({filename})."
        if required:
            raise ImportError(msg) from err
        log.warning("%s Skipping optional step.", msg)
        return None


def _prepare_sys_path() -> None:
    root = str(ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)  # let scripts import db_utils, s3_utils, etc. from repo root
    _alias("db_utils", "nhl_beyond27.db.utils")
    _alias("s3_utils", "nhl_beyond27.s3_utils")
    _alias("log_utils", "nhl_beyond27.logging_utils")


def _alias(name: str, target: str) -> None:
    if name in sys.modules:
        return
    try:
        mod = import_module(target)
    except ModuleNotFoundError:
        return
    sys.modules[name] = mod


def _load_by_path(path: Path):
    spec = importlib_util.spec_from_file_location(path.stem, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load spec for {path}") from None
    mod = importlib_util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[assignment]
    return mod


def _import_or_path(module_name: str, filename: str, required: bool = True):
    try:
        return import_module(module_name)
    except ModuleNotFoundError:
        path = ROOT / filename
        if path.exists():
            log.info("Loading %s from path: %s", module_name, path)
            return _load_by_path(path)
        msg = f"Missing module/file: {module_name} ({filename})."
        if required:
            raise ImportError(msg) from None
        log.warning("%s Skipping optional step.", msg)
        return None


# ----------------------------
# Rebuild pipeline (your existing steps)
# ----------------------------
def rebuild() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    _prepare_sys_path()

    # 1) Ingest (optional)
    ingest = _import_or_path("ingest_peak_season", "ingest_peak_season.py", required=False)
    if ingest:
        log.info("→ ingest_peak_season.main(upsert)")
        ingest.main(mode="upsert")

    # 2) Aligned table (required)
    aligned = _import_or_path(
        "build_player_streaks_and_aligned",
        "build_player_streaks_and_aligned.py",
        required=True,
    )
    log.info("→ build_player_streaks_and_aligned.main(rebuild=True)")
    aligned.main(rebuild=True, restrict_age_25_29=False)

    # 3) Player z (optional)
    z_player = _import_or_path(
        "build_player_five_year_aligned_z",
        "build_player_five_year_aligned_z.py",
        required=False,
    )
    if z_player:
        log.info("→ build_player_five_year_aligned_z.main(upsert)")
        z_player.main(mode="upsert")

    # 4) Cohort z (required)
    z_cohort = _import_or_path(
        "build_player_five_year_aligned_z_cohort",
        "build_player_five_year_aligned_z_cohort.py",
        required=True,
    )
    log.info("→ build_player_five_year_aligned_z_cohort.build(replace)")
    z_cohort.build(mode="replace")

    log.info("✅ Rebuild complete.")


def full(
    backup: bool = True,
    restore_path: str | None = None,
    *,
    backup_dbname: str = "nhl_beyond",
    backup_s3: str = "s3://ewnike-mads593-nhl/backups",
) -> None:
    """
    Run the full rebuild; optionally run a backup to S3 afterwards.
    - Set backup_dbname/s3 via args or env if you prefer.
    """
    rebuild()

    if backup:
        try:
            log.info("→ dump_db_to_s3(%s -> %s)", backup_dbname, backup_s3)
            dump_db_to_s3(backup_dbname, backup_s3)
            log.info("✅ Backup uploaded.")
        except Exception as e:
            log.error("Backup failed: %s", e)
            raise
