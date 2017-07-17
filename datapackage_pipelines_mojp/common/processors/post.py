from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor
from datapackage_pipelines_mojp.common.constants import PIPELINES_ES_DOC_TYPE
import elasticsearch
import elasticsearch.helpers
import logging


class CommonPostProcessor(FilterResourcesProcessor):

    def __init__(self, *args, **kwargs):
        super(CommonPostProcessor, self).__init__(*args, **kwargs)
        self._es = elasticsearch.Elasticsearch(self._get_settings("MOJP_ELASTICSEARCH_DB"))
        self._idx = self._get_settings("MOJP_ELASTICSEARCH_INDEX")

    def _filter_resource(self, resource, descriptor):
        if descriptor["name"] == "dbs_docs_sync_log":
            all_valid_ids = []
            for row in resource:
                all_valid_ids.append("{}_{}".format(row["source"], row["id"]))
                yield row
            self._es.indices.refresh(self._idx)
            items = elasticsearch.helpers.scan(self._es, index=self._idx,
                                               doc_type=PIPELINES_ES_DOC_TYPE, scroll=u"3h")
            for item in items:
                is_match = True
                for k, v in self._parameters["all_items_query"].items():
                    if item["_source"][k] != v:
                        is_match = False
                        break
                if is_match:
                    if item["_id"] not in all_valid_ids:
                        # item is in ES but not in the synced items, delete it!
                        self._es.delete(index=self._idx, doc_type=PIPELINES_ES_DOC_TYPE, id=item["_id"])
                        logging.info("delete es doc id {}".format(item["_id"]))
                    else:
                        pass
                        # it's possible to add post processing code here

if __name__ == "__main__":
    CommonPostProcessor.main()
