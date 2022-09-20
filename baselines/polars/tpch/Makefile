SHELL=/bin/bash
PYTHON=.venv/bin/python

.venv:
	@python -m venv .venv
	@.venv/bin/pip install -U pip
	@.venv/bin/pip install --no-cache-dir -r requirements.txt

clean-venv:
	rm -r .venv

clean: clean-venv

run_polars: .venv
	.venv/bin/python -m polars_queries.executor

pre-commit:
	.venv/bin/python -m isort .
	.venv/bin/python -m black .
