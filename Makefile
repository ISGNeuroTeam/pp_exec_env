#.SILENT:
TESTS_PTH = /tests
BASE_PTH = $(shell pwd)

venv:
	echo Create venv
	python3 -m venv ./venv
	./venv/bin/pip install -r requirements.txt

install: venv
	echo Install package
	./venv/bin/pip install .

clean_venv:
	echo "Cleaning venv..."
	rm -rf ./venv

test: venv install
	echo "Testing..."
	./venv/bin/python3 -m doctest execution_environment/*.py -o ELLIPSIS

clean: clean_venv
	echo "Cleaning..."
