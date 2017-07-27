from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import json
import logging
import time
import os


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        # TODO change scheme
        return {"fields": [{"name": "name", "type": "string"},
                           {"name": "main_image_url", "type": "string",
                               "description": "stored in aws S3"},
                           {"name": "desc", "type": "string"},
                           {"name": "id", "type": "integer",
                            "description": "enumerated ID given when scraped"},
                           {"name": "approximate_date_taken", "type": "string",
                            "description": "Date of the photo, extracted from name. "},
                           {"name": "pictures", "type": "array", "description": "List of image IDs created by AWS S3"}]}

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
        title = item_data["Header"]["En"]
        main_image_url = item_data["main_image_url"]
        description = item_data["UnitText1"]["En"]
        entity_id = item_data["UnitId"]
        date_taken = item_data["PeriodDesc"]["En"]
        pictures = item_data["Pictures"]
        new_doc["pictures"] = dict(pictures)
        new_doc["name"] = title
        new_doc["main_image_url"] = str(main_image_url)
        new_doc["desc"] = description
        new_doc["id"] = int(entity_id)
        new_doc["approximate_date_taken"] = date_taken

        return new_doc


if __name__ == '__main__':
    Processor.main()
