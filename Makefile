SHELL := /bin/bash
VENV := .venv
PIP := $(VENV)/bin/pip
PYTHON := $(VENV)/bin/python
ROWS ?= 1e7
ENGINES ?= duckdb,polars,teide
TEIDE_PY_REPO := https://github.com/TeideDB/teide-py.git

.PHONY: setup data bench clean

$(VENV)/bin/activate:
	python3 -m venv $(VENV)

setup: $(VENV)/bin/activate
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -e .
	@# Try PyPI first, fall back to source build
	@$(PIP) install -q teide 2>/dev/null \
		|| ( echo "teide not on PyPI, building from source..." \
			&& ( [ -d .deps/teide-py ] || git clone --depth 1 $(TEIDE_PY_REPO) .deps/teide-py ) \
			&& $(PIP) install -q .deps/teide-py )
	@echo "Setup complete."

data: setup
	@$(PYTHON) gen/generate.py --rows $(ROWS)

bench: data
	$(PYTHON) bench.py --rows $(ROWS) --engines $(ENGINES)

clean:
	rm -rf $(VENV) .deps datasets results.json
