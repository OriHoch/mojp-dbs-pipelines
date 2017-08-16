from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.processors.dump_to_sql import Processor as DumpToSqlProcessor
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
import logging, time


CLEARMASH_FOLDERS_SCHEMA = {"fields": [{"name": "folder_id", "type": "integer"},
                                       {"name": "metadata", "type": "object"}],
                            "primaryKey": ["folder_id"]}

CLEARMASH_ENTITY_IDS_SCHEMA = {"fields": [{"name": "item_id", "type": "integer"},
                                          {"name": "collection", "type": "string"},
                                          {"name": "metadata", "type": "object"},
                                          {"name": "folder_id", "type": "integer"}],
                               "primaryKey": ["item_id"]}


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._override_collections = self._get_settings("OVERRIDE_CLEARMASH_COLLECTIONS")
        if self._override_collections:
            logging.info("OVERRIDE_CLEARMASH_COLLECTIONS = {}".format(self._override_collections))
        self.folders_processor = DumpToSqlProcessor.initialize(self._parameters["folder-ids-table"],
                                                               CLEARMASH_FOLDERS_SCHEMA, self._get_settings())

    @classmethod
    def _get_schema(cls):
        return CLEARMASH_ENTITY_IDS_SCHEMA

    def _parse_folder_res(self, res, parent_folder, folder_id):
        for folder in res["Folders"]:
            yield from self._get_folder(folder["Id"], folder_metadata=folder)
        for item in res["Items"]:
            if not item["IsFolder"]:
                yield {"collection": parent_folder["collection"], "item_id": item["Id"],
                       "folder_id": folder_id, "metadata": item}

    def _get_folder(self, folder_id, folder=None, folder_metadata=None):
        if folder is None:
            folder = {"collection": "unknown"}
        res = self._get_clearmash_api().get_web_document_system_folder(folder_id)
        self.folders_processor.commit_rows([{"folder_id": folder_id, "metadata": folder_metadata}])
        yield from self._parse_folder_res(res, folder, folder_id)

    def _get_resource(self):
        for folder_id, folder in CONTENT_FOLDERS.items():
            if self._override_collections and folder["collection"] not in self._override_collections:
                logging.info("collection {} not in override collections, skipping".format(folder["collection"]))
                continue
            yield from self._get_folder(folder_id, folder)

    def _get_clearmash_api(self):
        return ClearmashApi()


if __name__ == '__main__':
    Processor.main()
