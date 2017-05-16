import json
from datapackage_pipelines_mojp.common.processors.sync import DBS_DOCS_TABLE_SCHEMA
from datapackage_pipelines_mojp.clearmash.constants import CLEARMASH_SOURCE_ID
from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor
import iso639
from datapackage_pipelines_mojp.common.utils import populate_iso_639_language_field


RESOURCE_NAME = "dbs_docs"
TABLE_SCHEMA = DBS_DOCS_TABLE_SCHEMA

class ClearmashConvertProcessor(FilterResourcesProcessor):

    def _filter_resource_descriptor(self, descriptor):
        if descriptor["name"] == "clearmash":
            descriptor.update(name=RESOURCE_NAME, path="{}.csv".format(RESOURCE_NAME), schema=TABLE_SCHEMA)
        return descriptor

    def _filter_row(self, row, resource_descirptor):
        return self._cm_row_to_dbs_row(row) if resource_descirptor["name"] == RESOURCE_NAME else row

    def _cm_row_to_dbs_row(self, cm_row):
        parsed_doc = json.loads(cm_row["parsed_doc"])
        dbs_row = {"source": CLEARMASH_SOURCE_ID,
                   "id": cm_row["item_id"],
                   "source_doc": json.dumps(cm_row),
                   "version": "{}-{}".format(cm_row["changeset"], cm_row["document_id"]),}
        populate_iso_639_language_field(dbs_row, "title", parsed_doc["entity_name"])
        populate_iso_639_language_field(dbs_row, "content_html", parsed_doc["_c6_beit_hatfutsot_bh_base_template_description"])
        return dbs_row


if __name__ == '__main__':
    ClearmashConvertProcessor.main()
