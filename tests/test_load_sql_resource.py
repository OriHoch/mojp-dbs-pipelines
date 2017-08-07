from datapackage_pipelines_mojp.common.processors.load_sql_resource import LoadSqlResource as Processor
from tests.common import get_test_db_session, get_mock_settings, assert_conforms_to_schema
from datapackage_pipelines_mojp.common.db import get_reflect_metadata
import os, tempfile, json, datetime


class MockProcessor(Processor):

    def _get_new_db_session(self):
        return self._test_db_session

def run_load_sql_resource_processor(data_in_db, session=None):
    if session is None:
        session = get_test_db_session()
    metadata = get_reflect_metadata(session)
    table = metadata.tables["clearmash-entities"]
    for row in data_in_db:
        table.insert().values(**row).execute()
    session.commit()
    schema_fields = [{"name": "item_id", "type": "integer"},
                     {"name": "last_synced", "type": "datetime"},
                     {"name": "hours_to_next_sync", "type": "integer"}]
    with tempfile.NamedTemporaryFile("w") as f:
        json.dump({"name": "_", "resources": [{"name": "entities",
                                               "schema": {"fields": schema_fields,
                                                          "primaryKey": ["item_id"]}}]}, f)
        f.flush()
        settings = get_mock_settings()
        parameters = {"add-resource": "entities",
                      "datapackage": f.name,
                      "load-resource": "entities",
                      "load-table": "clearmash-entities",
                      "where": "hours_to_next_sync=0"}
        datapackage = {"resources": []}
        resources = []
        processor = MockProcessor(parameters, datapackage, resources, settings)
        processor._test_db_session = session
        datapackage, resources, stats = processor.spew()
        resources = list(resources)
        assert len(resources) == 1
        resource = list(resources[0])
        for doc in resource:
            assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
    return resource

def test_no_data_in_db():
    resource = run_load_sql_resource_processor(data_in_db=[])
    assert len(resource) == 0

def test_data_in_db():
    datetime_now = datetime.datetime.now()
    resource = run_load_sql_resource_processor(data_in_db=[{"item_id": 1,
                                                            "last_synced": datetime_now,
                                                            "hours_to_next_sync": 0},
                                                           {"item_id": 2,
                                                            "last_synced": None,
                                                            "hours_to_next_sync": 5},
                                                           {"item_id": 3,
                                                            "last_synced": None,
                                                            "hours_to_next_sync": 0}])
    assert len(resource) == 2
    assert resource[0] == {"item_id": 1, "last_synced": datetime_now, "hours_to_next_sync": 0}
    assert resource[1] == {"item_id": 3, "last_synced": None, "hours_to_next_sync": 0}

def test_many_items():
    assert len(run_load_sql_resource_processor(data_in_db=[{"item_id": i, "last_synced": None, "hours_to_next_sync": 0}
                                                           for i in range(23)])) == 23
    assert len(run_load_sql_resource_processor(data_in_db=[{"item_id": i, "last_synced": None, "hours_to_next_sync": 0}
                                                           for i in range(111)])) == 111
    assert len(run_load_sql_resource_processor(data_in_db=[{"item_id": i, "last_synced": None, "hours_to_next_sync": 0}
                                                           for i in range(100)])) == 100
