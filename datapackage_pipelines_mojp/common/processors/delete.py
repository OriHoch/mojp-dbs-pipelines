from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.utils import get_elasticsearch
from datapackage_pipelines_mojp.common import constants
from elasticsearch import NotFoundError
import logging
import elasticsearch.helpers



class Processor(BaseProcessor):

    STATS_DELETED = "total items deleted in ES"

    def _filter_resource(self, resource_descriptor, resource_data):
        self._es, self._idx = get_elasticsearch(self._settings)
        self._stats[self.STATS_DELETED] = 0
        self._all_es_ids = self._get_all_es_ids()
        yield from super(Processor, self)._filter_resource(resource_descriptor, resource_data)

    def _get_all_es_ids(self):
        # TODO: optimize, this is really inefficient (but, done only once per pipeline run)
        return [doc["_id"] for doc in elasticsearch.helpers.scan(self._es, index=self._idx,
                                                                 doc_type=constants.PIPELINES_ES_DOC_TYPE,
                                                                 scroll=u"3h")]


    def _delete(self, id):
        if id in self._all_es_ids:
            self._es.delete(index=self._idx, doc_type=constants.PIPELINES_ES_DOC_TYPE, id=id)
            self._stats[self.STATS_DELETED] += 1
        else:
            self._stats[self.STATS_DELETED] += 1

    def _filter_row(self, row):
        id = row[self._parameters["id-field"]]
        source = self._parameters["source"]
        es_id = "{}_{}".format(id, source)
        if self._parameters.get("delete-all-input"):
            delete = True
        else:
            delete = not row["display_allowed"]
        if delete:
            self._delete(es_id)
        return row


if __name__ == '__main__':
    Processor.main()
