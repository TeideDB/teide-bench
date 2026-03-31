SHELL := /bin/bash
VENV := .venv
PIP := $(VENV)/bin/pip
PYTHON := $(VENV)/bin/python
ROWS ?= 1e7
K ?= 100
SEED ?= 0
ENGINES ?= duckdb,polars,glaredb,teide,rayforce
TEIDE_PY_REPO := https://github.com/TeideDB/teide-py.git
TEIDE_REPO := https://github.com/TeideDB/teide.git
RAYFORCE_PY_REPO := https://github.com/RayforceDB/rayforce-py.git
RAYFORCE_REPO := https://github.com/RayforceDB/rayforce.git

.PHONY: setup data bench clean

$(VENV)/bin/activate:
	python3 -m venv $(VENV)

setup: $(VENV)/bin/activate
	$(PIP) install -q --upgrade pip
	$(PIP) install -q duckdb polars glaredb
	@# Always build teide from latest git source
	@( [ -d .deps/teide-py ] \
		&& git -C .deps/teide-py fetch -q origin \
		&& git -C .deps/teide-py reset -q --hard origin/master \
		|| git clone --depth 1 $(TEIDE_PY_REPO) .deps/teide-py )
	@( [ -d .deps/teide-py/vendor/teide ] \
		&& git -C .deps/teide-py/vendor/teide fetch -q origin \
		&& git -C .deps/teide-py/vendor/teide reset -q --hard origin/master \
		|| git clone --depth 1 $(TEIDE_REPO) .deps/teide-py/vendor/teide )
	$(PIP) install -q --no-cache-dir --force-reinstall .deps/teide-py
	@# Always build rayforce-py from latest git source
	@( [ -d .deps/rayforce-py ] \
		&& git -C .deps/rayforce-py fetch -q origin \
		&& git -C .deps/rayforce-py reset -q --hard origin/master \
		|| git clone --depth 1 $(RAYFORCE_PY_REPO) .deps/rayforce-py )
	@rm -rf .deps/rayforce-py/tmp/rayforce-c && mkdir -p .deps/rayforce-py/tmp
	@git clone --depth 1 $(RAYFORCE_REPO) .deps/rayforce-py/tmp/rayforce-c
	@cp -r .deps/rayforce-py/tmp/rayforce-c/core .deps/rayforce-py/rayforce/rayforce
	@cd .deps/rayforce-py && make patch_rayforce_makefile rayforce_binaries
	$(PIP) install -q --no-cache-dir --force-reinstall .deps/rayforce-py
	@echo "Setup complete."

data: setup
	@$(PYTHON) -c "from gen.generate import parse_sci, dataset_prefix, join_dir_name, n_label; \
		import os; n=parse_sci('$(ROWS)'); k=$(K); s=$(SEED); \
		gb=os.path.join('datasets', dataset_prefix(n,k,s), dataset_prefix(n,k,s)+'.csv'); \
		ns=n_label(n); jx=os.path.join('datasets', join_dir_name(n), f'J1_{ns}_NA_0_0.csv'); \
		exit(0 if os.path.exists(gb) and os.path.exists(jx) else 1)" 2>/dev/null \
		|| $(PYTHON) gen/generate.py --rows $(ROWS) --k $(K) --seed $(SEED)

bench: data
	$(PYTHON) bench.py --rows $(ROWS) --k $(K) --seed $(SEED) --engines $(ENGINES)

clean:
	rm -rf $(VENV) .deps datasets results.json
