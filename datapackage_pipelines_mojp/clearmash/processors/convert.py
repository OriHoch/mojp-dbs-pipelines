from datapackage_pipelines_mojp.common.constants import DBS_DOCS_TABLE_SCHEMA
from datapackage_pipelines_mojp.clearmash.constants import CLEARMASH_SOURCE_ID
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, ClearmashRelatedDocuments
from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import populate_iso_639_language_field
import logging


class Processor(BaseProcessor):

    @classmethod
    def _get_schema(self):
        return DBS_DOCS_TABLE_SCHEMA

    def _filter_resource(self, resource_descriptor, resource_data):
        for cm_row in resource_data:
            dbs_row = self._cm_row_to_dbs_row(cm_row)
            if self._doc_show_filter(dbs_row):
                self._add_related_documents(dbs_row, cm_row)
                yield dbs_row

    def _get_clearmash_api(self):
        return ClearmashApi()

    def _doc_show_filter(self, dbs_row):
        working_status = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_working_status', [{}])[0].get("en")
        rights = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_rights', [{}])[0].get("en")
        display_status = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_display_status', [{}])[0].get("en")
        if bool(working_status == 'Completed' and rights == 'Full' and display_status not in ['Internal Use']):
            return True
        else:
            logging.info("item '{}' failed show filter '{}'/'{}'/'{}'".format(dbs_row["id"], working_status, rights, display_status))
            return False

    def _get_related_documents(self, cm_row):
        related_documents_config = self._parameters.get("related-documents")
        if related_documents_config:
            table = self.db_meta.tables.get(related_documents_config["table"])
            if table is not None:
                item_id_col = getattr(table.c, related_documents_config["item-id-column"])
                document_id_col = getattr(table.c, related_documents_config["document-id-column"])
                # aggregate all the document ids from all the fields - to allow fetching them from DB in one query
                all_document_ids = []
                # we also build this res which is similar to the res we will return but with document ids instead of item id
                # later we will convert known document ids to item ids
                document_id_res = {}
                for field_id, related_documents in self._get_clearmash_api().related_documents.get_for_doc(cm_row).items():
                    document_id_res[field_id] = []
                    for document_id in related_documents.first_page_results:
                        document_id_res[field_id].append(document_id)
                        if document_id not in all_document_ids:
                            all_document_ids.append(document_id)
                if len(all_document_ids) > 0:
                    rows = self.db_session\
                        .query(item_id_col, document_id_col)\
                        .filter(document_id_col.in_(all_document_ids))\
                        .all()
                    item_ids = {row[1]: row[0] for row in rows}
                    res = {}
                    for field_id, related_document_ids in document_id_res.items():
                        related_item_ids = []
                        for document_id in related_document_ids:
                            if document_id in item_ids:
                                related_item_ids.append(item_ids[document_id])
                        if len(related_item_ids) > 0:
                            res[field_id] = ["{}_{}".format(CLEARMASH_SOURCE_ID, item_id) for item_id in related_item_ids]
                    return res
        return {}

    def _add_related_documents(self, dbs_row, cm_row):
        dbs_row["related_documents"] = self._get_related_documents(cm_row)

    def _cm_row_to_dbs_row(self, cm_row):
        parsed_doc = cm_row["parsed_doc"]
        dbs_row = {"source": CLEARMASH_SOURCE_ID,
                   "id": str(cm_row["item_id"]),
                   "source_doc": cm_row,
                   "version": "{}-{}".format(cm_row["changeset"], cm_row["document_id"]),
                   "collection": self._get_collection(cm_row),
                   "main_image_url": "",
                   "main_thumbnail_image_url": ""}
        populate_iso_639_language_field(dbs_row, "title", parsed_doc.get("entity_name"))
        populate_iso_639_language_field(dbs_row, "content_html", parsed_doc.get("_c6_beit_hatfutsot_bh_base_template_description"))
        return dbs_row

    def _get_collection(self, cm_row):
        return cm_row["collection"]


if __name__ == '__main__':
    Processor.main()
