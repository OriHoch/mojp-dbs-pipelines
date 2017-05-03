from mojp_dbs_pipelines.common.processors.base_processors import AddResourcesProcessor
import json


class ClearmashDownloadProcessor(AddResourcesProcessor):

    def _get_resource_descriptors(self):
        return [{"name": "clearmash",
                 "path": "clearmash.csv",
                 "schema": {"fields": [
                     {"name": "id", "type": "integer"},
                     {"name": "source_doc", "type": "string"}
                 ]}}]

    def _mock_download(self):
        for o in [{"id": 1, "source_doc": json.dumps({"title": "foobar", "content": "bazbax"})},
                  {"id": 2, "source_doc": json.dumps({"title": "222", "content": "2222"})}]:
            yield o

    def _get_resources_iterator(self):
        return [self._mock_download()]


if __name__ == '__main__':
    ClearmashDownloadProcessor.main()
