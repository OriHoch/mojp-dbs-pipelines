from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.processors.dump_to_sql import Processor as DumpToSqlProcessor
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
import logging, time


CLEARMASH_FOLDERS_SCHEMA = {"fields": [{"name": "parent_id", "type": "integer"},
                                       {"name": "id", "type": "integer"},
                                       {"name": "name", "type": "string"},
                                       {"name": "metadata", "type": "object"}],
                            "primaryKey": ["id"]}

CLEARMASH_ENTITY_IDS_SCHEMA = {"fields": [{"name": "folder_id", "type": "integer"},
                                          {"name": "item_id", "type": "integer"},
                                          {"name": "collection", "type": "string"},
                                          {"name": "name", "type": "string"},
                                          {"name": "metadata", "type": "object"},],
                               "primaryKey": ["item_id"]}


class Processor(BaseProcessor):

    @classmethod
    def _get_schema(cls):
        return CLEARMASH_ENTITY_IDS_SCHEMA

    def _parse_folder_res(self, res, parent_folder, folder_id):
        for folder in res["Folders"]:
            yield from self._get_folder(folder["Id"], folder_metadata=folder, parent_folder_id=folder_id)
        for item in res["Items"]:
            if not item["IsFolder"]:  # sometimes folders appear in the items
                yield self._get_item(item, folder_id, parent_folder)

    def _get_item(self, item, folder_id, parent_folder):
        res = {"folder_id": folder_id,
                "item_id": item["Id"],
                "collection": parent_folder["collection"],
                "name": item["Name"],
                "metadata": item}
        self._stats["processed items"] += 1
        return res

    def _get_folder(self, folder_id, folder=None, folder_metadata=None, parent_folder_id=None):
        is_root_folder = folder is not None
        if folder is None:
            folder = {"collection": "unknown"}
        # add the row for this folder
        self.folders_buffer.append({"parent_id": parent_folder_id,
                                    "id": folder_id,
                                    "name": folder_metadata.get("Name", "") if folder_metadata else None,
                                    "metadata": folder_metadata})
        self._flush_folders(force=is_root_folder)
        self.log_progress()
        res = self._get_clearmash_api().get_web_document_system_folder(folder_id)
        self.log_progress()
        # recursively adds sub-folders and items
        yield from self._parse_folder_res(res, folder, folder_id)
        self._stats["processed folders"] += 1

    def _get_resource(self):
        self._override_collections = self._get_settings("OVERRIDE_CLEARMASH_COLLECTIONS")
        if self._override_collections:
            logging.info("OVERRIDE_CLEARMASH_COLLECTIONS = {}".format(self._override_collections))
        self.folders_processor = DumpToSqlProcessor.initialize(self._parameters["folders-table"],
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
        self._flush_folders(force=True)

    def _get_clearmash_api_class(self):
        return ClearmashApi

    def _get_clearmash_api(self):
        return self._get_clearmash_api_class()(keepalive_callback=lambda: self.log_progress(), stats=self._stats)

    def _flush_folders(self, force=False):
        commit_every = int(self._parameters.get("folders-commit-every", 10))
        if len(self.folders_buffer) > 0 and (force or len(self.folders_buffer) > commit_every):
            self.folders_processor.commit_rows(self.folders_buffer)
            self._stats.update({"folders: {}".format(k): v
                                for k, v in self.folders_processor.stats.items()})
            self.folders_buffer = []


if __name__ == '__main__':
    Processor.main()
