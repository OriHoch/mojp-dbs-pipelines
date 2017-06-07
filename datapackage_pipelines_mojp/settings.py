from dotenv import load_dotenv, find_dotenv
import os
from datapackage_pipelines.manager.logging_config import logging
from contextlib import contextmanager


DEFAULT_LOGGING_LEVEL = logging.INFO

root_logging_handler = logging.root.handlers[0]
root_logging_handler.setFormatter(logging.Formatter("%(levelname)s:%(module)s:%(message)s"))
root_logging_handler.setLevel(DEFAULT_LOGGING_LEVEL)

@contextmanager
def temp_loglevel(level):
    root_logging_handler.setLevel(level)
    yield
    root_logging_handler.setLevel(DEFAULT_LOGGING_LEVEL)


load_dotenv(find_dotenv())


def is_true(k):
    return os.environ.get(k, "").lower() in ["1", "true", "yes"]


CLEARMASH_CLIENT_TOKEN = os.environ.get("CLEARMASH_CLIENT_TOKEN")

MOJP_ELASTICSEARCH_DB = os.environ.get("MOJP_ELASTICSEARCH_DB")
MOJP_ELASTICSEARCH_INDEX = os.environ.get("MOJP_ELASTICSEARCH_INDEX")
MOJP_ELASTICSEARCH_DOCTYPE = os.environ.get("MOJP_ELASTICSEARCH_DOCTYPE", "common")

MOJP_ONLY_DOWNLOAD = is_true("MOJP_ONLY_DOWNLOAD")
MOJP_MOCK = is_true("MOJP_MOCK")
