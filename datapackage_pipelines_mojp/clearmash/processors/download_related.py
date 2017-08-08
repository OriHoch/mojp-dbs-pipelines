from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.clearmash.processors.download import CLEARMASH_DOWNLOAD_SCHEMA
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
from datapackage_pipelines_mojp.clearmash.common import doc_show_filter, check_download_ttl, update_download_ttl
from datapackage_pipelines_mojp.clearmash.constants import TEMPLATE_ID_COLLECTION_MAP
import logging, datetime


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        # mapping of str(document id) to int(item id) (for all entities)
        self._existing_document_ids = {}
        # mapping of int(item id) -> (last_downloaded, hours_to_next_download, last_synced) (for all entities)
        self._existing_item_ids = {}
        # override item ids are used to filter both the parent and related documents
        self._override_item_ids = self._parameters.get("override-item-ids")
        if self._override_item_ids:
            self._override_item_ids = self._override_item_ids.split(",")
            logging.info("using override-item-ids from parameters: {}".format(self._override_item_ids))
        else:
            self._override_item_ids = self._get_settings("OVERRIDE_CLEARMASH_ITEM_IDS")
            if self._override_item_ids:
                logging.info("using OVERRIDE_CLEARMASH_ITEM_IDS env var: {}".format(self._override_item_ids))

    def _process(self, *args, **kwargs):
        self._db_table = self._parameters.get("table")
        if self._db_table:
            self._db_table = self.db_meta.tables.get(self._db_table)
            if self._db_table is not None:
                query = self.db_session.query(self._db_table.c.item_id,
                                              self._db_table.c.last_downloaded,
                                              self._db_table.c.hours_to_next_download,
                                              self._db_table.c.last_synced,
                                              self._db_table.c.document_id).all()
                for row in query:
                    self._existing_item_ids[int(row.item_id)] = (row.last_downloaded,
                                                                 row.hours_to_next_download,
                                                                 row.last_synced)
                    self._existing_document_ids[row.document_id] = int(row.item_id)
                logging.info("{} existing item ids".format(len(self._existing_item_ids)))
                logging.info("{} existing document ids".format(len(self._existing_document_ids)))
        else:
            self._db_table = None
        return super(Processor, self)._process(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        return CLEARMASH_DOWNLOAD_SCHEMA

    def _get_clearmash_api(self):
        return ClearmashApi()

    def _check_override_parent_item(self, row):
        # this checks if parent item is allowed
        if not self._override_item_ids or str(row["item_id"]) in self._override_item_ids:
            # either no override item ids, or item is included in override item ids
            if row["collection"] and row["collection"] != "unknown":
                # main item from known collection
                return True
            else:
                # item without collection or with unknown collection are most likely related documents
                # to optimize we currently don't download relateds of relateds
                # TODO: enable downloading relateds of relateds (check if it's needed or if it's a problem to do)
                self._warn_once("items without collection are skipped")
                return False
        else:
            # parent item is not in override item ids
            self._warn_once("items are skipped to override ids env")
            return False

    def _check_override_related_item(self, related_document_id):
        # check if the related documents of an item are allowed
        related_item_id = self._existing_document_ids.get(related_document_id)
        return check_download_ttl(self._existing_item_ids, related_item_id)

    def _fetch_related_documents(self, related_documents):
        # make the api call to get related documents
        for doc in related_documents.get_related_documents():
            last_synced, hours_to_next_download = None, None
            document_id = doc["document_id"]
            item_id = self._existing_document_ids.get(document_id)
            last_synced, hours_to_next_download = update_download_ttl(self._existing_item_ids, item_id)
            if hours_to_next_download is not None:
                # this signifies not to download again, regardless of ttls
                self._existing_item_ids[item_id] = True
                self._existing_document_ids[document_id] = item_id
                doc.update(collection=TEMPLATE_ID_COLLECTION_MAP.get(doc["template_id"], "unknown"),
                           last_downloaded=datetime.datetime.now(),
                           hours_to_next_download=hours_to_next_download,
                           last_synced=last_synced,
                           display_allowed=doc_show_filter(doc["parsed_doc"]))
                yield doc

    def _filter_resource(self, resource_descriptor, resource_data):
        for row in resource_data:
            if self._check_override_parent_item(row):
                for field_id, related_documents in self._get_clearmash_api().related_documents.get_for_doc(row).items():
                    if related_documents.total_count < 50:
                        related_document_ids = related_documents.first_page_results
                        if len(related_document_ids) > 0:
                            num_to_download = 0
                            for related_document_id in related_document_ids:
                                if self._check_override_related_item(related_document_id):
                                    num_to_download += 1
                            if num_to_download > 0:
                                logging.info("fetching {} docs for {} / {}".format(num_to_download,
                                                                                   related_documents.entity_id,
                                                                                   field_id))
                                yield from self._fetch_related_documents(related_documents)
                            else:
                                self._warn_once("items are not fetched because they were already downloaded")
                        # else:
                          # no related documents
                    else:
                        self._warn_once("items are skipped because they return too many related documents")
            else:
                self._warn_once("items are skipped due to negative response from check_override_parent_item")

if __name__ == '__main__':
    Processor.main()
