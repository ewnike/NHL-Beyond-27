#!/usr/bin/env bash
set -euo pipefail
tree -a -I 'venv|.venv|__pycache__|.git|.mypy_cache|.pytest_cache|backups|*.dump' > docs/project-structure.txt
echo "Updated docs/project-structure.txt"

