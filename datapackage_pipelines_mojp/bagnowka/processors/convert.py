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
                   "main_thumbnail_image_url": cm_row["main_thumbnail_url"],
                   "title_en": cm_row["name"],
                   "title_he": "",
                   "content_html": {},
                   "content_html_en": cm_row["desc"],
                   "content_html_he": "",
                   "related_documents": self.creat_img_urls(cm_row),
                   "source_doc": cm_row
                   }
        return dbs_row

    def creat_img_urls(self, cm_row):
        images = {}
        count = 0
        ids = cm_row["pictures"]["picture_ids"]
        full_size_bucket_base_url = "https://s3-us-west-2.amazonaws.com/bagnowka-scraped/full/"
        thumb_bucket_base_url = "https://s3-us-west-2.amazonaws.com/bagnowka-scraped/thumbs/small/"
        for img_id in ids:
            count += 1
            img_url = "{}{}.jpg".format(full_size_bucket_base_url, img_id)
            thumb_url = "{}{}.jpg".format(thumb_bucket_base_url, img_id)
            images["{}".format(count)] = {"img_id": img_id, "img_url": img_url, "thumbnail_url": thumb_url}
        return images


if __name__ == '__main__':
    BagnowkaConvertProcessor.main()
