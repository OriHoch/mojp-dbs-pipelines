from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, parse_clearmash_documents
from datapackage_pipelines_mojp.clearmash.constants import (DOWNLOAD_PROCESSOR_BUFFER_LENGTH,
                                                            TEMPLATE_ID_COLLECTION_MAP)
from datapackage_pipelines_mojp.clearmash.common import doc_show_filter, check_download_ttl, update_download_ttl
import logging, datetime


CLEARMASH_DOWNLOAD_SCHEMA = {"fields": [{"name": "document_id", "type": "string",
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
                                         "description": "all other attributes"},
                                        {"name": "last_downloaded", "type": "datetime"},
                                        {"name": "hours_to_next_download", "type": "integer"},
                                        {"name": "last_synced", "type": "datetime"},
                                        {"name": "display_allowed", "type": "boolean"}],
                             "primaryKey": ["item_id"]}

class Processor(BaseProcessor):

    STATS_DOWNLOADED = "items downloaded from clearmash"
    STATS_ALLOWED = "items allowed for display"
    STATS_NOT_ALLOWED = "items not allowed for display"

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._rows_buffer = []
        self._override_item_ids = self._parameters.get("override-item-ids")
        if self._override_item_ids:
            self._override_item_ids = self._override_item_ids.split(",")
            logging.info("using override-item-ids from parameters: {}".format(self._override_item_ids))
        else:
            self._override_item_ids = self._get_settings("OVERRIDE_CLEARMASH_ITEM_IDS")
            if self._override_item_ids:
                logging.info("using OVERRIDE_CLEARMASH_ITEM_IDS env var: {}".format(self._override_item_ids))

    def _process(self, *args, **kwargs):
        self._existing_ids = {}
        self._db_table = self._parameters.get("table")
        if self._db_table:
            self._db_table = self.db_meta.tables.get(self._db_table)
            if self._db_table is not None:
                # TODO:  optimize
                query = self.db_session.query(self._db_table.c.item_id,
                                              self._db_table.c.last_downloaded,
                                              self._db_table.c.hours_to_next_download,
                                              self._db_table.c.last_synced).all()
                self._existing_ids = {int(row.item_id): (row.last_downloaded,
                                                         row.hours_to_next_download,
                                                         row.last_synced)
                                      for row in query}
        return  super(Processor, self)._process(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        return CLEARMASH_DOWNLOAD_SCHEMA

    def _filter_resource(self, resource_descriptor, resource_data):
        self._stats[self.STATS_DOWNLOADED] = 0
        self._stats[self.STATS_ALLOWED] = 0
        self._stats[self.STATS_NOT_ALLOWED] = 0
        for row in resource_data:
            if self._check_override_item(row):
                self._rows_buffer.append(row)
            yield from self._handle_rows_buffer()
        yield from self._handle_rows_buffer(force_flush=True)

    def _handle_rows_buffer(self, force_flush=False):
        if force_flush or len(self._rows_buffer) > DOWNLOAD_PROCESSOR_BUFFER_LENGTH:
            yield from self._flush_rows_buffer()

    def _check_override_item(self, row):
        if not self._override_item_ids or str(row["item_id"]) in self._override_item_ids:
            if self._db_table is None or int(row["item_id"]) not in self._existing_ids:
                return True
            else:
                return check_download_ttl(self._existing_ids, row["item_id"])
        else:
            return False

    def _flush_rows_buffer(self):
        item_ids = {row["item_id"]: row["collection"] for row in self._rows_buffer}
        self._rows_buffer = []
        if len(item_ids.keys()) > 0:
            for doc in parse_clearmash_documents(self._get_clearmash_api().get_documents(list(item_ids.keys()))):
                last_synced, hours_to_next_download = update_download_ttl(self._existing_ids, doc["item_id"])
                doc.update(collection=TEMPLATE_ID_COLLECTION_MAP.get(doc["template_id"], "unknown"),
                           last_downloaded=datetime.datetime.now(),
                           hours_to_next_download=hours_to_next_download,
                           last_synced=last_synced,
                           display_allowed=doc_show_filter(doc["parsed_doc"]))
                self._stats[self.STATS_DOWNLOADED] += 1
                if doc["display_allowed"]:
                    self._stats[self.STATS_ALLOWED] += 1
                else:
                    self._stats[self.STATS_NOT_ALLOWED] += 1
                yield doc

    def _get_clearmash_api(self):
        return ClearmashApi()

if __name__ == '__main__':
    Processor.main()
