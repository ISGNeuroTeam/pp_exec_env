cd `dirname "${BASH_SOURCE[0]}"`
cd pp_exec_env
mkdir -p venv
tar -xzf ../venv.tar.gz -C venv
. venv/bin/activate
conda-unpack
