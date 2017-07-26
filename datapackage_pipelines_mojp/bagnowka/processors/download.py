from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import json
import logging
import time


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        # TODO change scheme
        return {"fields": [{"name": "source", "type": "string"}, {"name": "collection", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "main_image_url_aws_s3", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "id", "type": "integer"},
            {"name": "date_taken", "type": "string"},
            {"name": "parsed_doc", "type": "object"}]}


    def _get_resource(self):
        all_items_file = open("/Users/libi/mojp-dbs-pipelines/datapackage_pipelines_mojp/bagnowka/processors/bagnowka_all.json", "r")
        all_docs = json.load(all_items_file)
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
        new_doc["name"] = title
        new_doc["source"] = "Bagnowka"
        new_doc["collection"] = "photoUnits"
        new_doc["main_image_url_aws_s3"] = main_image_url
        new_doc["description"] = description
        new_doc["id"] = int(entity_id)
        new_doc["date_taken"] = date_taken
        new_doc["parsed_doc"] = item_data

        return new_doc
        

if __name__ == '__main__':
    Processor.main()
