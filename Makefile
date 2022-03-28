ifndef VERBOSE
.SILENT:
endif

VERSION := $(shell cat setup.py | grep version | head -n 1 | sed -re "s/[^\"']+//" | sed -re "s/[\"',]//g")
BRANCH := $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')

CONDA_FOLDER = ./conda
CONDA = $(CONDA_FOLDER)/miniconda/bin/conda
ENV_NAME = pp_exec_env
ENV = $(CONDA_FOLDER)/miniconda/envs/$(ENV_NAME)
ENV_PYTHON = $(ENV)/bin/python3.9

download_conda:
	echo Download Miniconda
	mkdir -p $(CONDA_FOLDER)
	if [ ! -f $(CONDA_FOLDER)/miniconda.sh ]; then \
		wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.11.0-Linux-x86_64.sh -O $(CONDA_FOLDER)/miniconda.sh; \
	fi;

install_conda: download_conda
	echo Install Miniconda
	if [ ! -d $(CONDA_FOLDER)/miniconda/ ]; then \
		bash $(CONDA_FOLDER)/miniconda.sh -b -p $(CONDA_FOLDER)/miniconda; \
  	fi;

install_conda_pack:
	$(CONDA) install conda-pack -c defaults -y

create_env: install_conda install_conda_pack
	echo Create environment
	$(CONDA) env update -f build_environment.yml --prune
	$(ENV_PYTHON) -m pip install -r requirements.txt \
	--extra-index-url http://s.dev.isgneuro.com/repository/ot.platform/simple \
	--trusted-host s.dev.isgneuro.com

build: create_env
	echo Build

doctest: build
	echo Run doctests
	(export PYTHONPATH=./pp_exec_env/; $(ENV_PYTHON) -m doctest ./pp_exec_env/*.py -o NORMALIZE_WHITESPACE -o ELLIPSIS)

unittests: build
	echo Run unittests
	export PYTHONPATH="./tests/:./pp_exec_env/"; $(ENV_PYTHON) -m unittest discover -s tests/ -p '*.py'

test: doctest unittests

clean_dist:
	echo Clean dist folders
	rm -fr pp_exec_env.egg-info
	rm -fr build
	rm -fr dist
	rm -f $(ENV_NAME)-*.tar.gz venv.tar.gz
	rm -f prepare.sh

clean_conda:
	echo Clean Conda
	- if [ -d $(CONDA_FOLDER) ]; then \
		$(CONDA) env remove -n $(ENV_NAME) -y; \
  	fi;

remove_conda:
	echo Remove Conda
	if [ -d $(CONDA_FOLDER) ]; then \
		rm -fr $(CONDA_FOLDER); \
  	fi;

clean: clean_conda remove_conda clean_dist

publish: build
	( \
	. $(CONDA_FOLDER)/miniconda/bin/activate; \
	conda activate $(ENV_NAME); \
	python ./setup.py sdist; \
	)

make_prepare_sh:
	echo Create prepare.sh
	echo "\
	mkdir -p venv\n\
	tar -xzf venv.tar.gz -C venv\n\
	. venv/bin/activate\n\
	conda-unpack\
	" > prepare.sh

pack: build make_prepare_sh
	rm -f $(ENV_NAME)-*.tar.gz venv.tar.gz
	echo Create archive \"$(ENV_NAME)-$(VERSION)-$(BRANCH).tar.gz\"
	( \
	. $(CONDA_FOLDER)/miniconda/bin/activate; \
	conda pack -n pp_exec_env -o venv.tar.gz \
	)
	tar czf ./$(ENV_NAME)-$(VERSION)-$(BRANCH).tar.gz $(ENV_NAME) docs venv.tar.gz prepare.sh
	rm -f venv.tar.gz
