from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import get_elasticsearch
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, parse_clearmash_documents
from datapackage_pipelines_mojp.clearmash.constants import DOWNLOAD_PROCESSOR_BUFFER_LENGTH


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._es, self._idx = get_elasticsearch(self._settings)
        self._rows_buffer = []

    @classmethod
    def _get_schema(cls):
        return {"fields": [{"name": "document_id", "type": "string",
                            "description": "some sort of internal GUID"},
                           {"name": "item_id", "type": "integer",
                            "description": "the item id as requested from the folder"},
                           {"name": "item_url", "type": "string",
                            "description": "url to view the item in CM"},
                           {"name": "collection", "type": "string",
                            "description": "common dbs docs collection string"},
                           {"name": "template_changeset_id", "type": "integer",
                            "description": "I guess it's the id of template when doc was saved"},
                           {"name": "template_id", "type": "string",
                            "description": "can help to identify the item type"},
                           {"name": "changeset", "type": "integer",
                            "description": ""},
                           {"name": "metadata", "type": "object",
                            "description": "full metadata"},
                           {"name": "parsed_doc", "type": "object",
                            "description": "all other attributes"}]}

    def _filter_resource(self, resource_descriptor, resource_data):
        for row in resource_data:
            self._rows_buffer.append(row)
            yield from self._handle_rows_buffer()
        yield from self._handle_rows_buffer(force_flush=True)

    def _handle_rows_buffer(self, force_flush=False):
        if force_flush or len(self._rows_buffer) > DOWNLOAD_PROCESSOR_BUFFER_LENGTH:
            yield from self._flush_rows_buffer()

    def _check_override_item(self, row):
        override_item_ids = self._get_settings("OVERRIDE_CLEARMASH_ITEM_IDS")
        if not override_item_ids:
            return True
        else:
            return str(row["item_id"]) in override_item_ids

    def _flush_rows_buffer(self):
        item_ids = {row["item_id"]: row["collection"]
                    for row in self._rows_buffer
                    if self._check_override_item(row)}
        self._rows_buffer = []
        if len(item_ids.keys()) > 0:
            for doc in parse_clearmash_documents(self._get_clearmash_api().get_documents(list(item_ids.keys()))):
                doc["collection"] = item_ids[doc["item_id"]]
                yield doc

    def _get_clearmash_api(self):
        return ClearmashApi()

if __name__ == '__main__':
    Processor.main()
