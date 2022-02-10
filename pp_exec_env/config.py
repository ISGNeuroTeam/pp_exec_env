import configparser
import logging
import os


default_ini = """
[system_commands]
data_file_name = data
schema_file_name = _SCHEMA
sys_read_interproc_name = sys_read_interproc
sys_write_interproc_name = sys_write_interproc
sys_write_result_name = sys_write_result
local_storage_alias = LOCAL_POST_PROCESSING
shared_storage_alias = SHARED_POST_PROCESSING
interproc_storage_alias = INTERPROC_STORAGE

[plugins]
follow_symlinks = yes

[logging]
base_logger = PostProcessing
"""


def load_config():
    config = configparser.ConfigParser()
    config.read_string(default_ini)
    if not (file := config.read(os.environ.get("PP_ENV_CONFIG", "config.ini"))):
        logger = logging.getLogger(config["logging"]["base_logger"])
        logger.warning("No config file was found, using default settings")
    else:
        logger = logging.getLogger(config["logging"]["base_logger"])
        logger.info(f"Loaded config file {file[0]}")
    return config


if __name__ == "__main__":
    import io
    config = load_config()
    s = io.StringIO()
    config.write(s)
    s.seek(0)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(config["logging"]["base_logger"])
    logging.info(f"Config:\n{s.read()}")
