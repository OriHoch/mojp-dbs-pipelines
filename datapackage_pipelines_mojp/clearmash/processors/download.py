from datapackage_pipelines_mojp.common.processors.base_processors import BaseDownloadProcessor
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, MockClearMashApi, parse_clearmash_documents
from datapackage_pipelines_mojp.clearmash.constants import (CONTENT_FOLDERS, DOWNLOAD_TABLE_SCHEMA,
                                                            ITEM_IDS_BUFFER_LENGTH)
import os
from datapackage_pipelines_mojp.common.constants import PIPELINES_ES_DOC_TYPE
import elasticsearch
import elasticsearch.helpers
import logging


class ClearmashDownloadProcessor(BaseDownloadProcessor):

    def __init__(self, *args, **kwargs):
        super(ClearmashDownloadProcessor, self).__init__(*args, **kwargs)
        self._es = elasticsearch.Elasticsearch(self._get_settings("MOJP_ELASTICSEARCH_DB"))
        self._idx = self._get_settings("MOJP_ELASTICSEARCH_INDEX")

    def _get_clearmash_api(self):
        return ClearmashApi()

    def _get_mock_clearmash_api(self):
        return MockClearMashApi()

    def _get_source_name(self):
        return "clearmash"

    def _get_schema(self):
        return DOWNLOAD_TABLE_SCHEMA

    def _download(self, clearmash_api=None):
        if not clearmash_api:
            clearmash_api = self._get_clearmash_api()
        if self._parameters.get("related"):
            items = elasticsearch.helpers.scan(self._es, index=self._idx,
                                               doc_type=PIPELINES_ES_DOC_TYPE, scroll=u"3h")
            for item in items:
                entity_id = item["_source"]["item_id"]
                for k, v in item.items():
                    if k.startswith("related_documents_"):
                        field_id = k.replace("related_documents_", "")
                        logging.info(entity_id)
                        logging.info(field_id)
                        logging.info(v)
        elif self._parameters.get("folder_id") and os.environ.get("CLEARMASH_OVERRIDE_ITEM_IDS"):
            self.item_ids_buffer = list(map(int, os.environ["CLEARMASH_OVERRIDE_ITEM_IDS"].split(",")))
            folder = CONTENT_FOLDERS[self._parameters["folder_id"]]
            yield from self._handle_item_ids_buffer(folder, clearmash_api, force_flush=True)
        else:
            for folder_id, folder in CONTENT_FOLDERS.items():
                if self._parameters.get("folder_id", "") == "" or self._parameters["folder_id"] == folder_id:
                    self.item_ids_buffer = []
                    for item in clearmash_api.get_web_document_system_folder(folder_id)["Items"]:
                        self.item_ids_buffer.append(item["Id"])
                        yield from self._handle_item_ids_buffer(folder, clearmash_api)
                    yield from self._handle_item_ids_buffer(folder, clearmash_api, force_flush=True)

    def _mock_download(self):
        yield from self._download(clearmash_api=self._get_mock_clearmash_api())

    def _handle_item_ids_buffer(self, folder, clearmash_api, force_flush=False):
        if force_flush or len(self.item_ids_buffer) > ITEM_IDS_BUFFER_LENGTH:
            for item in parse_clearmash_documents(clearmash_api.get_documents(self.item_ids_buffer)):
                item["collection"] = folder["collection"]
                yield item
            self.item_ids_buffer = []


if __name__ == '__main__':
    ClearmashDownloadProcessor.main()
