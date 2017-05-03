from .common import assert_processor
from mojp_dbs_pipelines.clearmash.processors.download import ClearmashDownloadProcessor
from mojp_dbs_pipelines.clearmash.processors.convert import ClearmashConvertProcessor
import json


def test_download():
    assert_processor(
        ClearmashDownloadProcessor,
        parameters={},
        datapackage={"resources": []},
        resources=[],
        expected_datapackage={
            "resources": [{
                "name": "clearmash",
                "path": "clearmash.csv",
                "schema": {"fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "source_doc", "type": "string"}
                ]}
            }]
        },
        expected_resources=[[
            {"id": 1, "source_doc": json.dumps({"title": "foobar", "content": "bazbax"})},
            {"id": 2, "source_doc": json.dumps({"title": "222", "content": "2222"})}
        ]]
    )


def test_convert_to_dbs_documents():
    assert_processor(
        ClearmashConvertProcessor,
        parameters={},
        datapackage={
            "resources": [{
                "name": "clearmash",
                "path": "clearmash.csv",
                "schema": {"fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "source_doc", "type": "string"}
                ]}
            }]
        },
        resources=[[
            {"id": 1, "source_doc": json.dumps({"title": "foobar", "content": "bazbax"})},
            {"id": 2, "source_doc": json.dumps({"title": "222", "content": "2222"})}
        ]],
        expected_datapackage={
            "resources": [{
                "name": "dbs_docs",
                "path": "dbs_docs.csv",
                "schema": {"fields": [
                    {"name": "source", "type": "string"},
                    {"name": "id", "type": "string"},
                    {'name': 'version', 'type': 'string', 'description': 'source dependant field, used by sync process to detect document updates'},
                    {"name": "source_doc", "type": "string"}
                ]}
            }]
        },
        expected_resources=[[
            {"source": "clearmash", "id": "1", "version": "5",
             "source_doc": '{"title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": true}'},
            {"source": "clearmash", "id": "2", "version": "5",
             "source_doc": '{"title": "222", "content": "2222", "implemented": "not yet", "sorry": true}'}
        ]]
    )
