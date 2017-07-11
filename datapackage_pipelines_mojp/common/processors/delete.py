from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor
from datapackage_pipelines_mojp.common.constants import PIPELINES_ES_DOC_TYPE
import elasticsearch
import elasticsearch.helpers
import logging


class CommonDeleteProcessor(FilterResourcesProcessor):

    def __init__(self, *args, **kwargs):
        super(CommonDeleteProcessor, self).__init__(*args, **kwargs)
        self._es = elasticsearch.Elasticsearch(self._get_settings("MOJP_ELASTICSEARCH_DB"))
        self._idx = self._get_settings("MOJP_ELASTICSEARCH_INDEX")

    def _filter_resource(self, resource, descriptor):
        if descriptor["name"] == "dbs_docs_sync_log":
            all_valid_ids = []
            for row in resource:
                all_valid_ids.append("{}_{}".format(row["source"], row["id"]))
                yield row
            self._delete_all_docs_not_in(all_valid_ids)

    def _delete_all_docs_not_in(self, all_valid_ids):
        items = elasticsearch.helpers.scan(self._es, index=self._idx,
                                           doc_type=PIPELINES_ES_DOC_TYPE, scroll=u"3h")
        for item in items:
            if item["_id"] not in all_valid_ids:
                ok_to_delete = True
                for k, v in self._parameters["all_items_query"].items():
                    if item["_source"][k] != v:
                        ok_to_delete = False
                        break
                if ok_to_delete:
                    self._es.delete(index=self._idx, doc_type=PIPELINES_ES_DOC_TYPE, id=item["_id"])
                    logging.info("delete es doc id {}".format(item["_id"]))

if __name__ == "__main__":
    CommonDeleteProcessor.main()
