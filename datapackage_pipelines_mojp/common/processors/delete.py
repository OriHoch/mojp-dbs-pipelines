from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import get_elasticsearch
from datapackage_pipelines_mojp.common import constants
from elasticsearch import NotFoundError
import logging



class Processor(BaseProcessor):

    def _filter_resource(self, resource_descriptor, resource_data):
        self._es, self._idx = get_elasticsearch(self._settings)
        self._stats["total_deleted_in_es"] = 0
        self._stats["total_delete_not_found_errors"] = 0
        yield from super(Processor, self)._filter_resource(resource_descriptor, resource_data)

    def _delete(self, id):
        try:
            self._es.delete(index=self._idx, doc_type=constants.PIPELINES_ES_DOC_TYPE, id=id)
            self._stats["total_deleted_in_es"] += 1
        except NotFoundError:
            self._stats["total_delete_not_found_errors"] += 1

    def _filter_row(self, row):
        id = row[self._parameters["id-field"]]
        source = self._parameters["source"]
        es_id = "{}_{}".format(id, source)
        logging.info(es_id)
        if self._parameters.get("delete-all-input"):
            delete = True
        else:
            delete = row["display_allowed"]
        if delete:
            self._delete(es_id)
            return None
        else:
            return row


if __name__ == '__main__':
    Processor.main()