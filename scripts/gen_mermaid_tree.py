#!/usr/bin/env python3
"""
This file is code that creates the project
layout for the git repo. You need to manually
run the script and then copy the mermaid file found
the docs folder, project-structure.mmd. Copy that and
paste it into the readme.

Eric Winiecke
September 2025.
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
from pathlib import Path

# ---------- CONFIG ----------
ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())

OUT = ROOT / "docs" / "project-structure.mmd"
FLOW_DIR = "LR"  # "TD" top-down or "LR" left-right
MAX_DEPTH = 3  # show directory tree to this depth

# show files only in these subtrees (with per-dir rules)
SHOW_FILES_IN = {
    "src/nhl_beyond27": {
        "suffixes": {".py"},
        "keep": {"__init__.py", "cli.py", "pipeline.py"},
        "max": 30,
    },
    "scripts": {"suffixes": {".py", ".sh"}, "keep": set(), "max": 30},
    "tests": {"suffixes": {".py"}, "keep": set(), "max": 20},
}
# always show these top-level files
TOP_LEVEL_KEEP = {
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    "LICENSE",
    "Makefile",
    ".gitignore",
}

# ignore noisy dirs anywhere
IGNORE_DIRS = {
    ".git",
    ".github",
    ".idea",
    ".vscode",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "env",
    "venv",
    ".venv",
    "node_modules",
    "dist",
    "build",
    "backups",
    "logs",
    "docs",  # we’ll still write into docs/
    "data",
}
# ignore dir name patterns
IGNORE_DIR_ENDSWITH = (".egg-info",)

# ignore noisy files
IGNORE_FILE_PATTERNS = ["*.dump", "*.dump.*", "*.log", "*.sha256", "CACHEDIR.TAG"]

# ignore selenium
IGNORE_DIRS |= {".selenium_profile_stathead"}  # nuke the whole profile

# optional: block any hidden Selenium-ish profiles in the future
IGNORE_DIR_PREFIXES = (".selenium",)  # treat any dir starting with this as ignorable

# --------------------------------


def relposix(p: Path) -> str:
    return "." if p == ROOT else p.relative_to(ROOT).as_posix()


def path_id(p: Path) -> str:
    rel = relposix(p)
    slug = rel.replace("/", "__").replace(".", "_").replace("-", "_").replace(" ", "_")
    return f"id_{slug if slug != '.' else ROOT.name}"


def want_dir(parent: Path, name: str) -> bool:
    if name.startswith(IGNORE_DIR_PREFIXES):
        return False
    if name in IGNORE_DIRS or any(name.endswith(sfx) for sfx in IGNORE_DIR_ENDSWITH):
        return False
    # don't descend into ignored paths (by prefix)
    full = parent / name
    rp = relposix(full)
    return not (rp != "." and any(rp == ig or rp.startswith(ig + "/") for ig in IGNORE_DIRS))


def which_show_rule(dirpath: Path):
    r = relposix(dirpath)
    for base, rule in SHOW_FILES_IN.items():
        if r == base or r.startswith(base + "/") or (r == "." and base == "."):
            return rule
    return None


def keep_file(p: Path, rule: dict | None, top_level: bool) -> bool:
    name = p.name
    if top_level and name in TOP_LEVEL_KEEP:
        return True
    if rule is None:
        return False
    if name.startswith("."):
        return False
    if any(fnmatch.fnmatch(name, pat) for pat in IGNORE_FILE_PATTERNS):
        return False
    if name in rule.get("keep", set()):
        return True
    return p.suffix.lower() in rule.get("suffixes", set())


# --- collect nodes/edges ---
nodes: dict[Path, str] = {}
labels: dict[str, str] = {}
edges: list[tuple[str, str]] = []


def add_node(p: Path):
    pid = nodes.get(p)
    if not pid:
        pid = path_id(p)
        nodes[p] = pid
        labels[pid] = p.name or p.anchor
    return pid


root_id = add_node(ROOT)

for dirpath, dirnames, filenames in os.walk(ROOT):
    dirpath = Path(dirpath)

    # depth cap
    depth = 0 if dirpath == ROOT else len(dirpath.relative_to(ROOT).parts)
    if depth >= MAX_DEPTH:
        dirnames[:] = []
        filenames = []

    # prune dirs in-place
    dirnames[:] = [d for d in dirnames if want_dir(dirpath, d)]

    parent_id = add_node(dirpath)

    # child dirs
    for d in sorted(dirnames):
        child = dirpath / d
        edges.append((parent_id, add_node(child)))

    # files (only in selected places + top-level keepers)
    rule = which_show_rule(dirpath)
    top_level = dirpath == ROOT
    kept = [f for f in sorted(filenames) if keep_file(dirpath / f, rule, top_level)]
    if rule and "max" in rule and len(kept) > rule["max"]:
        kept = kept[: rule["max"]]
    for f in kept:
        edges.append((parent_id, add_node(dirpath / f)))

# --- write Mermaid ---
OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open("w", encoding="utf-8") as w:
    w.write("```mermaid\n")
    w.write(f"flowchart {FLOW_DIR}\n")
    labels[nodes[ROOT]] = ROOT.name
    for nid, label in labels.items():
        safe = label.replace("[", "\\[").replace("]", "\\]")
        w.write(f'    {nid}["{safe}"]\n')
    for a, b in edges:
        w.write(f"    {a} --> {b}\n")
    w.write("```\n")

print(f"Wrote {OUT.relative_to(ROOT)} (depth ≤ {MAX_DEPTH})")
