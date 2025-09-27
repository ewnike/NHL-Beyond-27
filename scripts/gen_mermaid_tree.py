#!/usr/bin/env python3
import os
from pathlib import Path

# --- config ---
ROOT = Path(".").resolve()
OUT = Path("docs/project-structure.mmd")  # change if you like
MAX_DEPTH = 4  # keep the diagram readable

# folders to ignore entirely
IGNORE_DIRS = {
    ".git",
    ".github",
    ".vscode",
    ".idea",
    ".venv",
    "env",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "dist",
    "build",
    ".selenium",
    "logs",
    "data",  # comment this out if you want to show data/
    "src/nhl_beyond27.egg-info",
}
# files to ignore (patterns)
IGNORE_FILE_SUFFIX = {".pyc", ".pyo"}

# --- walk & collect edges ---
nodes = {}  # path -> node_id
labels = {}  # node_id -> label
edges = []  # (parent_id, child_id)
nid = 0


def add_node(p: Path):
    global nid
    if p not in nodes:
        nodes[p] = f"n{nid}"
        labels[nodes[p]] = p.name or p.anchor
        nid += 1
    return nodes[p]


def should_ignore_dir(dirpath: Path, name: str) -> bool:
    rel = dirpath.relative_to(ROOT)
    candidate = (rel / name).as_posix()
    # match simple names or known subpaths
    return (name in IGNORE_DIRS) or any(candidate.startswith(x) for x in IGNORE_DIRS)


root_id = add_node(ROOT)

for dirpath, dirnames, filenames in os.walk(ROOT):
    dirpath = Path(dirpath)
    # depth limit
    depth = len(dirpath.relative_to(ROOT).parts)
    if depth >= MAX_DEPTH:
        dirnames[:] = []  # don't descend further
        filenames = []

    # prune ignored directories in-place for os.walk
    dirnames[:] = [d for d in dirnames if not should_ignore_dir(dirpath, d)]

    # add current directory node
    parent_id = add_node(dirpath)

    # add child directories
    for d in sorted(dirnames):
        child_path = dirpath / d
        child_id = add_node(child_path)
        edges.append((parent_id, child_id))

    # add files (lightweight)
    for f in sorted(filenames):
        if any(f.endswith(sfx) for sfx in IGNORE_FILE_SUFFIX):
            continue
        # skip hidden files
        if f.startswith("."):
            continue
        child_path = dirpath / f
        child_id = add_node(child_path)
        edges.append((parent_id, child_id))

# --- write Mermaid flowchart ---
OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open("w", encoding="utf-8") as w:
    w.write("```mermaid\n")
    w.write("flowchart TD\n")
    # nicer label for repo root
    labels[nodes[ROOT]] = ROOT.name or "/"
    for node_id, label in labels.items():
        safe = label.replace("[", "\\[").replace("]", "\\]")
        w.write(f'    {node_id}["{safe}"]\n')
    for a, b in edges:
        w.write(f"    {a} --> {b}\n")
    w.write("```\n")

print(f"Wrote {OUT} (depth ≤ {MAX_DEPTH}, ignoring: {sorted(IGNORE_DIRS)})")
