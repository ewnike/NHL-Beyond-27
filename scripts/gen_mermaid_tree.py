# # #!/usr/bin/env python3
# from __future__ import annotations

# import fnmatch
# import os
# import subprocess
# from pathlib import Path

# # ---------- CONFIG ----------
# # repo root (robust even if run from subfolders)
# ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())

# OUT = ROOT / "docs" / "project-structure.mmd"  # where to write the Mermaid block
# MAX_DEPTH = 4  # folders deeper than this are collapsed
# MAX_FILES_PER_DIR = 50  # safety cap; set to None to disable

# SHOW_FILES = False  # <--- set this
# KEEP_BASENAMES = {"README.md", "pyproject.toml", "requirements.txt", "LICENSE", "Makefile"}

# # Hide these directories anywhere in the tree
# IGNORE_DIRS = {
#     ".git",
#     ".github",
#     ".vscode",
#     ".idea",
#     ".ruff_cache",
#     "__pycache__",
#     ".pytest_cache",
#     ".mypy_cache",
#     "env",
#     "venv",
#     ".venv",
#     "node_modules",
#     "dist",
#     "build",
#     ".selenium",
#     "logs",
#     "backups",  # <<< noisy backups, dumps, logs
#     "data",  # hide data/ by default
#     "docs",  # hide docs/ … (we'll re-allow diagrams below if wanted)
# }

# # Re-allow a subtree inside docs if you want to show diagrams
# ALLOW_DIRS = {
#     "docs/diagrams",  # keep Mermaid sources visible
# }

# # Only include files with these suffixes (keep code & docs concise)
# KEEP_SUFFIXES = {".py", ".ipynb", ".md", ".toml", ".yaml", ".yml"}

# # Also keep these specific file names (top-level helpers)
# KEEP_BASENAMES = {
#     "LICENSE",
#     "Makefile",
#     "requirements.txt",
#     "pyproject.toml",
#     ".gitignore",
#     "README.md",
# }

# # Ignore noisy file patterns (dumps, logs, checksums, caches, temp)
# IGNORE_FILE_PATTERNS = [
#     "*.dump",
#     "*.dump.*",
#     "*.log",
#     "*.sha256",
#     "CACHEDIR.TAG",
#     "project-structure.txt",
# ]

# # Optionally restrict top-level to a **whitelist** of dirs you care about.
# # Leave empty to keep any top-level dir (minus ignores).
# TOP_LEVEL_DIR_WHITELIST = {"src", "scripts", "tests", "notebooks"}
# # --------------------------------


# # --- internals ---
# nodes: dict[Path, str] = {}
# labels: dict[str, str] = {}
# edges: list[tuple[str, str]] = []
# nid = 0


# def add_node(p: Path) -> str:
#     global nid
#     if p not in nodes:
#         nodes[p] = f"n{nid}"
#         labels[nodes[p]] = p.name or p.anchor
#         nid += 1
#     return nodes[p]


# def is_allowed_dir(path: Path) -> bool:
#     """Return True if the directory should be walked."""
#     rel = path.relative_to(ROOT).as_posix()
#     # allow explicit ALLOW_DIRS even if they match an ignored prefix
#     for allow in ALLOW_DIRS:
#         if rel == allow or rel.startswith(allow + "/"):
#             return True
#     # otherwise block if any IGNORE_DIRS prefix matches
#     if rel in IGNORE_DIRS:
#         return False
#     return not any(rel.startswith(ign + "/") for ign in IGNORE_DIRS)


# def should_prune_dir(parent: Path, name: str) -> bool:
#     rel = (parent / name).relative_to(ROOT).as_posix()
#     # top-level whitelist
#     if name.endswith(".egg-info"):
#         return True
#     if parent == ROOT and TOP_LEVEL_DIR_WHITELIST:
#         if name not in TOP_LEVEL_DIR_WHITELIST and name not in IGNORE_DIRS:
#             return True
#     # general ignore logic
#     if name in IGNORE_DIRS:
#         return True
#     if not is_allowed_dir(parent / name):
#         return True
#     return False


# def keep_file(p: Path) -> bool:
#     # suffix or explicit basename
#     if p.name in KEEP_BASENAMES:
#         return True
#     if p.suffix.lower() in KEEP_SUFFIXES:
#         # still allow suffix, but filter noise patterns below
#         pass
#     else:
#         return False
#     # filter by noise patterns
#     for pat in IGNORE_FILE_PATTERNS:
#         if fnmatch.fnmatch(p.name, pat):
#             return False
#     # skip hidden files generally
#     if p.name.startswith(".") and p.name not in KEEP_BASENAMES:
#         return False
#     return True


# # walk
# root_id = add_node(ROOT)
# for dirpath, dirnames, filenames in os.walk(ROOT):
#     dirpath = Path(dirpath)

#     # depth control
#     depth = len(dirpath.relative_to(ROOT).parts)
#     if depth >= MAX_DEPTH:
#         dirnames[:] = []  # stop descent
#         filenames = []  # and don't list deep files

#     # prune directories in-place
#     dirnames[:] = [d for d in dirnames if not should_prune_dir(dirpath, d)]

#     # if the directory itself is not allowed, skip listing its files
#     if dirpath != ROOT and not is_allowed_dir(dirpath):
#         continue

#     parent_id = add_node(dirpath)

#     # child directories
#     for d in sorted(dirnames):
#         child_id = add_node(dirpath / d)
#         edges.append((parent_id, child_id))

#     # files (filtered)
#     if SHOW_FILES:
#         kept_files = [f for f in sorted(filenames) if keep_file(dirpath / f)]
#     else:
#         kept_files = []
#         # but still show select top-level keepers:
#         if dirpath == ROOT:
#             kept_files = [f for f in filenames if (dirpath / f).name in KEEP_BASENAMES]

#     if MAX_FILES_PER_DIR is not None and len(kept_files) > MAX_FILES_PER_DIR:
#         kept_files = kept_files[:MAX_FILES_PER_DIR]  # cap extremely busy dirs
#     for f in kept_files:
#         child_id = add_node(dirpath / f)
#         edges.append((parent_id, child_id))

# # write Mermaid
# OUT.parent.mkdir(parents=True, exist_ok=True)
# with OUT.open("w", encoding="utf-8") as w:
#     w.write("```mermaid\n")
#     w.write("flowchart TD\n")  # or 'LR' if you prefer wide diagrams
#     labels[nodes[ROOT]] = ROOT.name or "/"
#     for node_id, label in labels.items():
#         safe = label.replace("[", "\\[").replace("]", "\\]")
#         w.write(f'    {node_id}["{safe}"]\n')
#     for a, b in edges:
#         w.write(f"    {a} --> {b}\n")
#     w.write("```\n")

# print(f"Wrote {OUT.relative_to(ROOT)} (depth ≤ {MAX_DEPTH})")
#!/usr/bin/env python3
from __future__ import annotations
import os, fnmatch, subprocess
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
    if rp != "." and any(rp == ig or rp.startswith(ig + "/") for ig in IGNORE_DIRS):
        return False
    return True


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
    if p.suffix.lower() in rule.get("suffixes", set()):
        return True
    return False


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
