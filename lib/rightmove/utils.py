
import logging
import sys
import tempfile
from typing import Any
log = logging.getLogger('tracker')


def setup_logging():
    # TODO: Either get this to work or remove.
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)


def write_to_temp_file(contents:list[Any], file_prefix=None, file_suffix:str=None):
    with tempfile.NamedTemporaryFile(mode="w", prefix=file_prefix, suffix=file_suffix, delete=False) as fp:
        log.info(f"Writing data to temp file: {fp.name}")
        for con in contents:
            fp.write(str(con))

