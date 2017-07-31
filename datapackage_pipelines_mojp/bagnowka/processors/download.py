from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import json
import logging
import time
import os

class BagnowkaDownloadProcessor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(BagnowkaDownloadProcessor, self).__init__(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        return {"fields": [{"name": "name", "type": "string"},
                           {"name": "main_image_url", "type": "string",
                               "description": "stored in aws S3"},
                           {"name": "desc", "type": "string"},
                           {"name": "id", "type": "string",
                            "description": "enumerated ID given when scraped"},
                           {"name": "approximate_date_taken", "type": "string",
                            "description": "Date of the photo, extracted from name. "},
                           {"name": "pictures", "type": "object", "description": "List of image IDs created by AWS S3"}]}

    def _get_resource(self):
        with open(os.path.join(os.path.dirname(__file__), "bagnowka_all.json")) as f:
            all_docs = json.load(f)
            for item_data in all_docs:
                new = all_docs[item_data]
                doc = self.download(new)
                logging.info("hello world")
                yield doc


    def download(self, item_data):
        new_doc = {}
        description = item_data["UnitText1"]["En"]
        new_doc["desc"] = description
        new_doc["pictures"] = []
        title = item_data["Header"]["En"]
        main_image_url = item_data["main_image_url"]
        entity_id = str(item_data["UnitId"])
        date_taken = item_data["PeriodDesc"]["En"]
        all_pics = item_data["Pictures"]
        pic_ids = [i["PictureId"] for i in all_pics]
        new_doc["pictures"] = {"picture_ids": pic_ids}
        new_doc["name"] = title
        new_doc["main_image_url"] = main_image_url
        new_doc["id"] = entity_id
        new_doc["approximate_date_taken"] = date_taken

        return new_doc


if __name__ == '__main__':
    BagnowkaDownloadProcessor.main()
