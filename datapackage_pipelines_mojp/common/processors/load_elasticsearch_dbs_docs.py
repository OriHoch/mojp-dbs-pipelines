from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import get_elasticsearch
import elasticsearch.helpers
from datapackage_pipelines_mojp.common import constants


class Processor(BaseProcessor):

    def _get_schema(cls):
        return constants.DBS_DOCS_TABLE_SCHEMA

    def _get_resource(self):
        self._es, self._idx = get_elasticsearch(self._settings)
        self._idx_exists = self._es.indices.exists(self._idx)
        if self._idx_exists:
            for dbs_doc in elasticsearch.helpers.scan(self._es, index=self._idx,
                                                      doc_type=constants.PIPELINES_ES_DOC_TYPE,
                                                      scroll=u"3h"):
                row = self._filter_row(dbs_doc)
                if row:
                    yield row

    def _filter_row(self, dbs_doc):
        doc = dbs_doc["_source"]
        if doc["source"] == self._parameters["source"]:
            return doc


if __name__ == '__main__':
    Processor.main()
