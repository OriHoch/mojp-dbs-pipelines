from elasticsearch import Elasticsearch, NotFoundError

from datapackage_pipelines_mojp.common.processors.sync import CommonSyncProcessor
from .common import assert_processor

ELASTICSEARCH_TESTS_INDEX = "mojptests"

def initialize_elasticsearch():
    es = Elasticsearch("localhost:9200")
    try:
        es.indices.delete(ELASTICSEARCH_TESTS_INDEX)
    except NotFoundError:
        pass
    return es


def assert_mock_resources_sync(expected_resource):
    assert_processor(
        CommonSyncProcessor,
        mock_settings=type("MockSettings", (object,), {"MOJP_ELASTICSEARCH_DB": "localhost:9200",
                                                       "MOJP_ELASTICSEARCH_INDEX": ELASTICSEARCH_TESTS_INDEX}),
        parameters={},
        datapackage={
            'resources': [{
                'name': 'dbs_docs', 'path': 'dbs_docs.csv',
                'schema': {'fields': [
                    {"name": "source", "type": "string"},
                    {'name': 'id', 'type': 'string'},
                    {'name': 'version', 'type': 'string',
                     'description': 'source dependant field, used by sync process to detect document updates'},
                    {"name": "source_doc", "type": "string"}
                ]}
            }]
        },
        resources=[[
            {"source": "clearmash", "id": "1", "version": "5",
             "source_doc": '{"title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": true}'},
            {"source": "clearmash", "id": "2", "version": "5",
             "source_doc": '{"title": "222", "content": "2222", "implemented": "not yet", "sorry": true}'}
        ]],
        expected_datapackage={
            'resources': [{
                'name': 'dbs_docs_sync_log',
                'path': 'dbs_docs_sync_log.csv',
                'schema': {'fields': [
                    {"name": "source", "type": "string"},
                    {'name': 'id', 'type': 'string'},
                    {"name": "sync_msg", "type": "string"}
                ]}
            }]
        },
        expected_resources=[expected_resource]
    )


def test_initial_sync():
    es = initialize_elasticsearch()
    assert_mock_resources_sync([
        {"source": "clearmash", "id": "1", "version": "5", "sync_msg": "added to ES"},
        {"source": "clearmash", "id": "2", "version": "5", "sync_msg": "added to ES"}
    ])
    es.indices.refresh()
    assert es.get(ELASTICSEARCH_TESTS_INDEX, id="clearmash_1").get("_source") == {"version": "5", "title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": True}
    assert es.get(ELASTICSEARCH_TESTS_INDEX, id="clearmash_2").get("_source") == {"version": "5", "title": "222", "content": "2222", "implemented": "not yet", "sorry": True}


def test_update():
    es = initialize_elasticsearch()
    es.index(index=ELASTICSEARCH_TESTS_INDEX, doc_type="common", body={
        "title": "222", "content": "2222-version4", "version": "4",
        "implemented": "not yet", "sorry": True
    }, id="clearmash_2")
    es.indices.refresh()
    assert_mock_resources_sync([
        {"source": "clearmash", "id": "1", "version": "5", "sync_msg": "added to ES"},
        {"source": "clearmash", "id": "2", "version": "5", "sync_msg": "updated doc in ES (old version = \"4\")"}
    ])
    es.indices.refresh()
    assert es.get("mojptests", id="clearmash_1").get("_source") == {"title": "foobar", "content": "bazbax", "version": "5",
                                                                    "implemented": "not yet", "sorry": True}
    assert es.get("mojptests", id="clearmash_2").get("_source") == {"title": "222", "content": "2222", "version": "5",
                                                                    "implemented": "not yet", "sorry": True}
