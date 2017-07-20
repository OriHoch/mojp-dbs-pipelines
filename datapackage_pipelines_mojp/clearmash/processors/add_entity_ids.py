from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
import logging, time


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

    @classmethod
    def _get_schema(cls):
        return {"fields": [{"name": "collection", "type": "string"},
                           {"name": "item_id", "type": "integer"}]}

    def _get_folder(self, folder_id, folder, num_retries=0):
        if num_retries > 0:
            logging.info("sleeping {} seconds before retrying".format(self._get_settings("CLEARMASH_MAX_RETRIES")))
            time.sleep(self._get_settings("CLEARMASH_RETRY_SLEEP_SECONDS"))
        try:
            for item in self._get_clearmash_api().get_web_document_system_folder(folder_id)["Items"]:
                yield {"collection": folder["collection"], "item_id": item["Id"]}
        except Exception:
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

    def _get_resource(self):
        override_collections = self._get_settings("OVERRIDE_CLEARMASH_COLLECTIONS")
        for folder_id, folder in CONTENT_FOLDERS.items():
            if override_collections and folder["collection"] not in override_collections.split(","):
                continue
            yield from self._get_folder(folder_id, folder)

    def _get_clearmash_api(self):
        return ClearmashApi()


if __name__ == '__main__':
    Processor.main()
