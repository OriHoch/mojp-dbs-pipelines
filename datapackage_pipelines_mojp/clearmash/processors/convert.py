from datapackage_pipelines_mojp.common.processors.sync import (DBS_DOCS_TABLE_SCHEMA,
                                                               INPUT_RESOURCE_NAME as DBS_DOCS_RESOURCE_NAME)
from datapackage_pipelines_mojp.clearmash.constants import CLEARMASH_SOURCE_ID
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, parse_clearmash_documents
from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor
from datapackage_pipelines_mojp.common.utils import populate_iso_639_language_field
import logging


class ClearmashConvertProcessor(FilterResourcesProcessor):

    def __init__(self, *args, **kwargs):
        super(ClearmashConvertProcessor, self).__init__(*args, **kwargs)
        self.clearmash_api = ClearmashApi()

    def _filter_resource_descriptor(self, descriptor):
        if descriptor["name"] == "clearmash":
            descriptor.update(name=DBS_DOCS_RESOURCE_NAME,
                              path="{}.csv".format(DBS_DOCS_RESOURCE_NAME),
                              schema=DBS_DOCS_TABLE_SCHEMA)
        return descriptor

    def _doc_show_filter(self, dbs_row):
        working_status = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_working_status', [{}])[0].get("en")
        rights = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_rights', [{}])[0].get("en")
        display_status = dbs_row["source_doc"]["parsed_doc"].get('_c6_beit_hatfutsot_bh_base_template_display_status', [{}])[0].get("en")
        if bool(working_status == 'Completed' and rights == 'Full' and display_status not in ['Internal Use']):
            return True
        else:
            logging.info("item '{}' failed show filter '{}'/'{}'/'{}'".format(dbs_row["id"], working_status, rights, display_status))
            return False

    def _filter_row(self, row, resource_descriptor):
        if resource_descriptor["name"] == DBS_DOCS_RESOURCE_NAME:
            dbs_row = self._cm_row_to_dbs_row(row)
            if self._doc_show_filter(dbs_row):
                return dbs_row
            else:
                return None
        else:
            return row

    def get_item_id_from_doc_id(self, doc_id):
        # it's possible to optimize here by looking for existing doc ids in elasticsearch
        return None

    def _fetch_related_documents(self, cm_row, field_id, first_page_document_ids=None):
        item_ids = None
        if first_page_document_ids:
            item_ids = [item_id
                        for item_id in [self.get_item_id_from_doc_id(doc_id) for doc_id in first_page_document_ids]
                        if item_id]
            if len(item_ids) != len(first_page_document_ids):
                item_ids = None
        if not item_ids:
            entity_id = cm_row["item_id"]
            url = "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Document/ByRelationField"
            post_data = {"EntityId": entity_id, "FieldId": field_id, "MaxNestingDepth": 1}
            related_res = self.clearmash_api._get_request_json(url, self.clearmash_api._get_headers(), post_data)
            item_ids = [item["item_id"] for item in parse_clearmash_documents(related_res)]
        return ["{}_{}".format(CLEARMASH_SOURCE_ID, item_id)
                for item_id in item_ids]

    def _get_related_documents(self, cm_row):
        related_documents = {}
        for k, v in cm_row["parsed_doc"].items():
            if isinstance(v, (tuple, list)) and len(v) == 2 and v[0] == "RelatedDocuments":
                first_page_document_ids = v[1]["FirstPageOfReletedDocumentsIds"]
                total_items_count = v[1]["TotalItemsCount"]
                related_documents[k] = []
                if len(first_page_document_ids) > 0:
                    # got some related documents
                    if len(first_page_document_ids) < total_items_count:
                        # there are too many items, must make the get related documents api call
                        related_documents[k] = self._fetch_related_documents(cm_row, k)
                    else:
                        # it might be possible to skip the additional api call
                        related_documents[k] = self._fetch_related_documents(cm_row, k, first_page_document_ids)
        return related_documents


    def _cm_row_to_dbs_row(self, cm_row):
        parsed_doc = cm_row["parsed_doc"]
        dbs_row = {"source": CLEARMASH_SOURCE_ID,
                   "id": str(cm_row["item_id"]),
                   "source_doc": cm_row,
                   "version": "{}-{}".format(cm_row["changeset"], cm_row["document_id"]),
                   "collection": self._get_collection(cm_row),
                   "main_image_url": "",
                   "main_thumbnail_image_url": "",
                   "related_documents": self._get_related_documents(cm_row)}
        populate_iso_639_language_field(dbs_row, "title", parsed_doc.get("entity_name"))
        populate_iso_639_language_field(dbs_row, "content_html", parsed_doc.get("_c6_beit_hatfutsot_bh_base_template_description"))
        return dbs_row

    def _get_collection(self, cm_row):
        return cm_row["collection"]


if __name__ == '__main__':
    ClearmashConvertProcessor.main()
