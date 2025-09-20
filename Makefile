# .PHONY: install rebuild full tree clean

# # Find nb27 in the active Python env
# NB27 := $(shell python -c 'import shutil; print(shutil.which("nb27") or "")')
# ifeq ($(NB27),)
# $(error Can't find nb27 in PATH. Activate your venv, then run make again. Example: `pyenv activate nhl_beyond27-3.13.7`)
# endif

# install:
# 	pip install -e .

# rebuild: install
# 	$(NB27) rebuild

# full: install
# 	$(NB27) full

# tree:
# 	bash scripts/gen_tree.sh

# clean:
# 	find . -name '__pycache__' -type d -prune -exec rm -rf {} +

# .PHONY: db-dump db-restore
# db-dump:
# 	@AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) S3_BUCKET_NAME=$(S3_BUCKET_NAME) \
# 	PGHOST=$(PGHOST) PGPORT=$(PGPORT) PGUSER=$(PGUSER) PGPASSWORD=$(PGPASSWORD) PGDATABASE=$(PGDATABASE) \
# 	bash scripts/dump_db.sh

# db-restore:
# 	@AWS_PROFILE=$(AWS_PROFILE) AWS_REGION=$(AWS_REGION) S3_BUCKET_NAME=$(S3_BUCKET_NAME) \
# 	PGHOST=$(PGHOST) PGPORT=$(PGPORT) PGUSER=$(PGUSER) PGPASSWORD=$(PGPASSWORD) PGDATABASE=$(PGDATABASE) \
# 	RESET_DB=1 bash scripts/restore_latest_from_s3.sh

SHELL := /bin/bash

.PHONY: install rebuild full tree clean db-dump db-restore

# install the package (editable)
install:
	@pip install -e .

# Only check nb27 when we actually use it
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

# Use quotes on all envs so secrets with special chars are safe
ENV_VARS = AWS_PROFILE="$(AWS_PROFILE)" AWS_REGION="$(AWS_REGION)" S3_BUCKET_NAME="$(S3_BUCKET_NAME)" \
           PGHOST="$(PGHOST)" PGPORT="$(PGPORT)" PGUSER="$(PGUSER)" PGPASSWORD="$(PGPASSWORD)" PGDATABASE="$(PGDATABASE)"

db-dump:
	@$(ENV_VARS) bash scripts/dump_db.sh

# You can override TARGET_DB at call time: make db-restore TARGET_DB=nhl_beyond_test
db-restore:
	@$(ENV_VARS) RESET_DB=1 TARGET_DB="$(TARGET_DB)" bash scripts/restore_latest_from_s3.sh
