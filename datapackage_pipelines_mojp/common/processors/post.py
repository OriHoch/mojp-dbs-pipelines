from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage_pipelines_mojp.common.processors.sync import CommonSyncProcessor
from datapackage_pipelines_mojp.common.constants import PIPELINES_ES_DOC_TYPE
from datapackage_pipelines_mojp.common.utils import get_elasticsearch
import elasticsearch.helpers
import logging
import os


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._es, self._idx = get_elasticsearch(self._settings)

    def _get_schema(cls):
        return CommonSyncProcessor._get_schema()

    def _skip_delete(self):
        skip_if = self._parameters.get("skip-delete-if")
        if skip_if.get("environment"):
            for k in skip_if["environment"]:
                if os.environ.get(k):
                    logging.info("skipping delete due to environment variable {}".format(k))
                    return True
        return False


    def _filter_resource(self, resource_descriptor, resource_data):
        if self._skip_delete():
            yield from resource_data
        else:
            all_valid_ids = []
            for row in resource_data:
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
    Processor.main()
