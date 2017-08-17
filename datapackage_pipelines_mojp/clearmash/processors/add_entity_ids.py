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

    @classmethod
    def _get_schema(cls):
        return CLEARMASH_ENTITY_IDS_SCHEMA

    def _parse_folder_res(self, res, parent_folder, folder_id):
        for folder in res["Folders"]:
            yield from self._get_folder(folder["Id"], folder_metadata=folder)
        for item in res["Items"]:
            if not item["IsFolder"]:
                item = {"collection": parent_folder["collection"], "item_id": item["Id"],
                       "folder_id": folder_id, "metadata": item}
                self._stats["processed items"] += 1
                yield item

    def _get_folder(self, folder_id, folder=None, folder_metadata=None):
        if folder is None:
            folder = {"collection": "unknown"}
        self.log_progress()
        res = self._get_clearmash_api().get_web_document_system_folder(folder_id)
        self.log_progress()
        self.folders_buffer.append({"folder_id": folder_id, "metadata": folder_metadata})
        yield from self._parse_folder_res(res, folder, folder_id)
        if len(self.folders_buffer) > int(self._parameters.get("folders-commit-every", 10)):
            self.folders_processor.commit_rows(self.folders_buffer)
            self.folders_buffer = []
        self._stats["processed folders"] += 1

    def _get_resource(self):
        self._override_collections = self._get_settings("OVERRIDE_CLEARMASH_COLLECTIONS")
        if self._override_collections:
            logging.info("OVERRIDE_CLEARMASH_COLLECTIONS = {}".format(self._override_collections))
        self.folders_processor = DumpToSqlProcessor.initialize(self._parameters["folder-ids-table"],
                                                               CLEARMASH_FOLDERS_SCHEMA, self._get_settings())
        self.folders_buffer = []
        self._stats["processed root folders"] = 0
        self._stats["processed folders"] = 0
        self._stats["processed items"] = 0
        self.start_progress()
        for folder_id, folder in CONTENT_FOLDERS.items():
            if self._override_collections and folder["collection"] not in self._override_collections:
                logging.info("collection {} not in override collections, skipping".format(folder["collection"]))
                continue
            yield from self._get_folder(folder_id, folder)
            self._stats["processed root folders"] += 1
        if len(self.folders_buffer) > 0:
            self.folders_processor.commit_rows(self.folders_buffer)

    def _get_clearmash_api(self):
        return ClearmashApi(keepalive_callback=lambda: self.log_progress())


if __name__ == '__main__':
    Processor.main()
