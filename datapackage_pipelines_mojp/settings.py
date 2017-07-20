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

CLEARMASH_MAX_RETRIES = int(os.environ.get("OVERRIDE_CLEARMASH_MAX_RETRIES", "5"))
CLEARMASH_RETRY_SLEEP_SECONDS = int(os.environ.get("OVERRIDE_CLEARMASH_RETRY_SLEEP_SECONDS", "60"))

OVERRIDE_CLEARMASH_COLLECTIONS = os.environ.get("OVERRIDE_CLEARMASH_COLLECTIONS")
if OVERRIDE_CLEARMASH_COLLECTIONS:
    OVERRIDE_CLEARMASH_COLLECTIONS = OVERRIDE_CLEARMASH_COLLECTIONS.split(",")

OVERRIDE_CLEARMASH_ITEM_IDS = os.environ.get("OVERRIDE_CLEARMASH_ITEM_IDS")
if OVERRIDE_CLEARMASH_ITEM_IDS:
    OVERRIDE_CLEARMASH_ITEM_IDS = OVERRIDE_CLEARMASH_ITEM_IDS.split(",")
