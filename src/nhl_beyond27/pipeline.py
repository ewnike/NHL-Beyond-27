from __future__ import annotations

import logging
import sys
from importlib import import_module
from importlib import util as importlib_util
from pathlib import Path

log = logging.getLogger(__name__)
# repo root = .../NHL-Beyond-27
PROJ_ROOT = Path(__file__).resolve().parents[2]


def load_optional(module_name: str, filename: str, required: bool = False):
    try:
        mod = __import__(module_name, fromlist=["*"])
        return mod
    except Exception as err:  # capture the original exception
        msg = f"Missing module/file: {module_name} ({filename})."
        if required:
            # keep the original traceback attached (B904)
            raise ImportError(msg) from err
            # If you prefer to hide the original cause, use:
            # raise ImportError(msg) from None
        log.warning("%s Skipping optional step.", msg)
        return None


def _prepare_sys_path() -> None:
    root = str(PROJ_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)  # let scripts import db_utils, s3_utils, etc. from repo root
    # Soft-alias legacy module names to package modules if you moved them
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
        path = PROJ_ROOT / filename
        if path.exists():
            log.info("Loading %s from path: %s", module_name, path)
            return _load_by_path(path)
        msg = f"Missing module/file: {module_name} ({filename})."
        if required:
            raise ImportError(msg) from None
        log.warning("%s Skipping optional step.", msg)
        return None


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


def full(backup: bool = True, restore_path: str | None = None) -> None:
    rebuild()
