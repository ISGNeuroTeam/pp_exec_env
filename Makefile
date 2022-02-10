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

test: clean venv install
	@echo "Testing..."
	echo $(shell pwd)
	. venv/bin/activate
	(cd pp_exec_env ; python -m doctest ./*.py -o ELLIPSIS -o NORMALIZE_WHITESPACE)
	(cd tests; python -m unittest command_executor sys_commands)
	deactivate ./venv/bin/activate


clean: clean_venv
	@echo "Cleaning..."
