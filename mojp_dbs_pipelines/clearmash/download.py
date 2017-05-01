from mojp_dbs_pipelines import settings
import json

TABLE_SCHEMA = {"fields": [{"name": "id", "type": "integer"},
                           {"name": "doc", "type": "string"}]}

def download():
    if settings.CLEARMASH_MOCK_DOWNLOAD:
        yield from _mock_download()
    else:
        yield from _real_download()


def _mock_download():
    for o in [{"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})},
              {"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})}]:
        yield o


def _real_download():
    raise NotImplementedError()
