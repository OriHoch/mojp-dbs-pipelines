from .common import assert_processor
from mojp_dbs_pipelines.clearmash.processors.add_resources import AddClearmashResources
from mojp_dbs_pipelines.clearmash.processors.convert_to_dbs_documents import ConvertClearmashResources
import json


def test_add_resources():
    assert_processor(
        AddClearmashResources,
        parameters={},
        datapackage={"resources": []},
        resources=[],
        expected_datapackage={
            "resources": [{
                "name": "clearmash",
                "path": "clearmash.csv",
                "schema": {"fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "doc", "type": "string"}
                ]}
            }]
        },
        expected_resources=[[
            {"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})},
            {"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})}
        ]]
    )


def test_convert_to_dbs_documents():
    assert_processor(
        ConvertClearmashResources,
        parameters={},
        datapackage={
            "resources": [{
                "name": "clearmash",
                "path": "clearmash.csv",
                "schema": {"fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "doc", "type": "string"}
                ]}
            }]
        },
        resources=[[
            {"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})},
            {"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})}
        ]],
        expected_datapackage={
            "resources": [{
                "name": "dbs_docs",
                "path": "dbs_docs.csv",
                "schema": {"fields": [
                    {"name": "source", "type": "string"},
                    {"name": "id", "type": "string"},
                    {"name": "doc", "type": "string"}
                ]}
            }]
        },
        expected_resources=[[
            {"source": "clearmash", "id": "1", "doc": '{"title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": true}'},
            {"source": "clearmash", "id": "2", "doc": '{"title": "222", "content": "2222", "implemented": "not yet", "sorry": true}'}
        ]]
    )
