SHELL := /bin/bash

.PHONY: install rebuild full tree clean db-dump db-restore

install:
	@pip install -e .

rebuild: install
	@command -v nb27 >/dev/null || { echo "Can't find nb27 in PATH. Activate your venv (e.g. pyenv activate nhl_beyond27-3.13.7)"; exit 1; }
	@nb27 rebuild

full: install
	@command -v nb27 >/dev/null || { echo "Can't find nb27 in PATH. Activate your venv (e.g. pyenv activate nhl_beyond27-3.13.7)"; exit 1; }
	@nb27 full

tree:
	@bash scripts/gen_tree.sh

clean:
	@find . -name '__pycache__' -type d -prune -exec rm -rf {} +

# ---- DB helpers ----
ENV_VARS = AWS_PROFILE="$(AWS_PROFILE)" AWS_REGION="$(AWS_REGION)" S3_BUCKET_NAME="$(S3_BUCKET_NAME)" \
           PGHOST="$(PGHOST)" PGPORT="$(PGPORT)" PGUSER="$(PGUSER)" PGPASSWORD="$(PGPASSWORD)" PGDATABASE="$(PGDATABASE)"

db-dump:
	@$(ENV_VARS) bash scripts/dump_db.sh

# Override TARGET_DB at call time if desired (see README examples)
db-restore:
	@$(ENV_VARS) RESET_DB=1 TARGET_DB="$(TARGET_DB)" bash scripts/restore_latest_from_s3.sh
