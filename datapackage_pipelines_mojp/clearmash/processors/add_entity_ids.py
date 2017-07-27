from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
import logging, time


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._override_collections = self._get_settings("OVERRIDE_CLEARMASH_COLLECTIONS")
        if self._override_collections:
            self._override_collections = self._override_collections.split(",")
            logging.info("OVERRIDE_CLEARMASH_COLLECTIONS = {}".format(self._override_collections))

    @classmethod
    def _get_schema(cls):
        return {"fields": [{"name": "item_id", "type": "integer"},
                           {"name": "collection", "type": "string"}],
                "primaryKey": ["item_id"]}

    def _filter_row(self, row):
        return row

    def _get_folder(self, folder_id, folder, num_retries=0):
        if num_retries > 0:
            logging.info("sleeping {} seconds before retrying".format(self._get_settings("CLEARMASH_RETRY_SLEEP_SECONDS")))
            time.sleep(self._get_settings("CLEARMASH_RETRY_SLEEP_SECONDS"))
        try:
            res = self._get_clearmash_api().get_web_document_system_folder(folder_id)
        except Exception as e:
            logging.error(e)
            logging.info("unexpected exception getting clearmash folder for collection {}".format(folder["collection"]))
            num_retries += 1
            if num_retries < self._get_settings("CLEARMASH_MAX_RETRIES"):
                logging.info("retrying ({} / {})".format(num_retries, self._get_settings("CLEARMASH_MAX_RETRIES")))
                yield from self._get_folder(folder_id, folder, num_retries=num_retries)
            else:
                logging.info("reached max retries ({})".format(self._get_settings("CLEARMASH_MAX_RETRIES")))
                skip_failure_collections = self._parameters.get("skip-failure-collections")
                if skip_failure_collections and folder["collection"] in skip_failure_collections:
                    logging.info("collection is in skip-failure param, so continuing".format(self._get_settings("CLEARMASH_MAX_RETRIES")))
                else:
                    raise
        else:
            for item in res["Items"]:
                row = {"collection": folder["collection"], "item_id": item["Id"]}
                row = self._filter_row(row)
                if row:
                    yield self._filter_row(row)

    def _get_resource(self):
        for folder_id, folder in CONTENT_FOLDERS.items():
            if self._override_collections and folder["collection"] not in self._override_collections:
                logging.info("collection {} not in override collections, skipping".format(folder["collection"]))
                continue
            yield from self._get_folder(folder_id, folder)
        self._process_cleanup()

    def _get_clearmash_api(self):
        return ClearmashApi()


if __name__ == '__main__':
    Processor.main()
