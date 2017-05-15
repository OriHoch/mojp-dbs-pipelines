from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def is_true(k):
    return os.environ.get(k, "").lower() in ["1", "true", "yes"]


CLEARMASH_CLIENT_TOKEN = os.environ.get("CLEARMASH_CLIENT_TOKEN")
MOJP_ELASTICSEARCH_DB = os.environ.get("MOJP_ELASTICSEARCH_DB")
MOJP_ELASTICSEARCH_INDEX = os.environ.get("MOJP_ELASTICSEARCH_INDEX")
MOJP_ONLY_DOWNLOAD = is_true("MOJP_ONLY_DOWNLOAD")
MOJP_MOCK = is_true("MOJP_MOCK")
