import json
from copy import deepcopy
from datapackage_pipelines_mojp.common.processors.sync import DBS_DOCS_TABLE_SCHEMA
from datapackage_pipelines_mojp.clearmash.constants import CLEARMASH_SOURCE_ID
from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor


class ClearmashConvertProcessor(FilterResourcesProcessor):

    def _filter_resource_descriptor(self, descriptor):
        if descriptor["name"] == "clearmash":
            # replace clearmash documents resource with the common dbs docs resource
            descriptor.update({"name": "dbs_docs",
                               "path": "dbs_docs.csv",
                               "schema": DBS_DOCS_TABLE_SCHEMA})
        return descriptor

    def _filter_row(self, row, resource_descirptor):
        if resource_descirptor["name"] == "dbs_docs":
            # filter rows of clearmash documents and convert them to dbs docs documents
            clearmash_id = row["id"]
            clearmash_doc = json.loads(row["source_doc"])
            dbs_doc = deepcopy(clearmash_doc)
            # add some mock data to know that document was modified
            dbs_doc.update({"implemented": "not yet",
                            "sorry": True})
            row = {"source": CLEARMASH_SOURCE_ID,
                   "id": str(clearmash_id),
                   "source_doc": json.dumps(dbs_doc),
                   "version": "5"}
        return row


if __name__ == '__main__':
    ClearmashConvertProcessor.main()
