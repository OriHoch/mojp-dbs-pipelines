# datapackage_pipelines_mojp/bagnowka/convert.py

from datapackage_pipelines_mojp.common.constants import DBS_DOCS_TABLE_SCHEMA
from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import populate_iso_639_language_field
import logging


class BagnowkaConvertProcessor(BaseProcessor):

    @classmethod
    def _get_schema(self):
        return DBS_DOCS_TABLE_SCHEMA

    def _filter_resource(self, resource_descriptor, resource_data):
        for cm_row in resource_data:
            dbs_row = self._cm_row_to_dbs_row(cm_row)
            logging.info("hello CONVERT")
            yield dbs_row

    def _cm_row_to_dbs_row(self, cm_row):
        dbs_row = {"source": "Bagnowka",
                "id": cm_row["id"],
                "version": "one",
                "title": {"en": cm_row["name"]},
                "collection": "photoUnits",
                "main_image_url": cm_row["main_image_url"],
                "main_thumbnail_image_url": "",
                "title_en": cm_row["name"], 
                "title_he": "",
                "content_html": {"en": cm_row["desc"]},
                "content_html_en": "",
                "content_html_he": "",
                "related_documents": {},
                "source_doc": cm_row
                }
        return dbs_row

if __name__ == '__main__':
    BagnowkaConvertProcessor.main()
