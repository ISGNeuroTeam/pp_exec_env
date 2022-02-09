#.SILENT:
TESTS_PTH = /tests
BASE_PTH = $(shell pwd)

venv:
	@echo Create venv
	python3 -m venv ./venv
	./venv/bin/pip install -r requirements.txt

install: venv
	@echo Install package
	./venv/bin/pip install .

clean_venv:
	@echo "Cleaning venv..."
	rm -rf ./venv

test: venv install
	@echo "Testing..."
	(cd pp_exec_env ; ../venv/bin/python3 -m doctest ./*.py -o ELLIPSIS -o NORMALIZE_WHITESPACE)

clean: clean_venv
	@echo "Cleaning..."
