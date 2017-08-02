from datapackage_pipelines_mojp.common.processors.update_db import Processor
from tests.common import get_test_db_session, get_mock_settings, assert_resource_conforms_to_schema
from datapackage_pipelines_mojp.common.db import get_reflect_metadata
from tests.test_load_sql_resource import run_load_sql_resource_processor
import datetime

class MockProcessor(Processor):

    def _get_new_db_session(self):
        return self._test_db_session

def test():
    session = get_test_db_session()
    datetime_now = datetime.datetime.now()
    data_in_db = [{"item_id": 1, "last_synced": datetime_now, "hours_to_next_sync": 0},
                  {"item_id": 2, "last_synced": None, "hours_to_next_sync": 0}]
    load_sql_resource = run_load_sql_resource_processor(data_in_db=data_in_db, session=session)
    settings = get_mock_settings()
    parameters = {"resource": "entities",
                  "table": "clearmash-entities",
                  "id-column": "item_id",
                  "id-field": "item_id",
                  "fields":{
                      "last_synced": "(datetime:datetime.now)",
                      "hours_to_next_sync": 5}}
    fields = [{"name": "item_id", "type": "integer"},
              {"name": "last_synced", "type": "datetime"},
              {"name": "hours_to_next_sync", "type": "integer"}]
    datapackage = {"resources": [{"name": "entities", "schema": {"fields": fields}}]}
    processor = MockProcessor(parameters, datapackage, [load_sql_resource], settings)
    processor._test_db_session = session
    datapackage, resources = processor.spew()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    assert len(resource) == 2
    assert_resource_conforms_to_schema(datapackage["resources"][0]["schema"], resource)
    assert [d["item_id"] for d in resource] == [1, 2]
    assert [d["hours_to_next_sync"] for d in resource] == [5, 5]
    assert resource[0]["last_synced"] > datetime_now
    assert resource[1]["last_synced"] > datetime_now
    metadata = get_reflect_metadata(session)
    rows = session.query(metadata.tables["clearmash-entities"]).all()
    assert [r.item_id for r in rows] == [1, 2]
    assert [r.hours_to_next_sync for r in rows] == [5, 5]
    assert rows[0].last_synced > datetime_now
    assert rows[1].last_synced > datetime_now
