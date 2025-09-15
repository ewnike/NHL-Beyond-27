.PHONY: install rebuild full tree clean

# Find nb27 in the active Python env
NB27 := $(shell python -c 'import shutil; print(shutil.which("nb27") or "")')
ifeq ($(NB27),)
$(error Can't find nb27 in PATH. Activate your venv, then run make again. Example: `pyenv activate nhl_beyond27-3.13.7`)
endif

install:
	pip install -e .

rebuild: install
	$(NB27) rebuild

full: install
	$(NB27) full

tree:
	bash scripts/gen_tree.sh

clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
