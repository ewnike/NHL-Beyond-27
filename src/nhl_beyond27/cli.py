from __future__ import annotations
import argparse
from .pipeline import rebuild, full

def main() -> None:
    p = argparse.ArgumentParser(prog="nb27", description="NHL Beyond 27 pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    s_full = sub.add_parser("full", help="backup → reset+restore(optional) → rebuild")
    s_full.add_argument("--no-backup", action="store_true")
    s_full.add_argument("--restore-path")

    sub.add_parser("rebuild", help="rebuild artifacts on current DB")

    args = p.parse_args()
    if args.cmd == "rebuild":
        rebuild()
    elif args.cmd == "full":
        full(backup=not args.no_backup, restore_path=args.restore_path)
