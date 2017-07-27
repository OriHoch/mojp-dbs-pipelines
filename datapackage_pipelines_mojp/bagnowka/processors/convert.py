# datapackage_pipelines_mojp/bagnowka/convert.py

from datapackage_pipelines_mojp.common.constants import DBS_DOCS_TABLE_SCHEMA
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

    def _doc_show_filter(self, dbs_row):
            return True

    def _add_related_documents(self, dbs_row, cm_row):
        dbs_row["related_documents"] = {}

    def _cm_row_to_dbs_row(self, cm_row):
        dbs_row = {"source": "Bagnowka",
                   "id": self.cm_row["id"],
                   "source_doc": cm_row,
                   "version": "one",
                   "collection": "photoUnits",
                   "main_image_url": "",
                   "main_thumbnail_image_url": ""}
        populate_iso_639_language_field(dbs_row, "title", cm_row["name"])
        populate_iso_639_language_field(dbs_row, "content_html", cm_row["desc"])
        return dbs_row

if __name__ == '__main__':
    Processor.main()
