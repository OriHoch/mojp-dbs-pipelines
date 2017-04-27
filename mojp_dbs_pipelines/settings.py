from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


def is_true(k):
    return os.environ.get(k, "").lower() in ["1", "true", "yes"]


CLEARMASH_MOCK_DOWNLOAD = is_true("CLEARMASH_MOCK_DOWNLOAD")
CLEARMASH_CLIENT_TOKEN = os.environ.get("CLEARMASH_CLIENT_TOKEN")
