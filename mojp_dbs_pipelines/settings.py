from dotenv import load_dotenv, find_dotenv
import os

if os.environ.get("MOJP_PIPELINES_ENV"):
    dotenv_filename = ".env.{}".format(os.environ["MOJP_PIPELINES_ENV"])
else:
    dotenv_filename = ".env"
load_dotenv(find_dotenv(filename=dotenv_filename))

def is_true(k):
    return os.environ.get(k, "").lower() in ["1", "true", "yes"]

CLEARMASH_MOCK_DOWNLOAD = is_true("CLEARMASH_MOCK_DOWNLOAD")
CLEARMASH_CLIENT_TOKEN = os.environ.get("CLEARMASH_CLIENT_TOKEN")
