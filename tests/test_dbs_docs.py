from elasticsearch import Elasticsearch, NotFoundError
from datapackage_pipelines_mojp.common.processors.sync import CommonSyncProcessor, DBS_DOCS_TABLE_SCHEMA
from .common import assert_processor
import json

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
        datapackage={'resources': [{'name': 'dbs_docs', 'path': 'dbs_docs.csv', 'schema': DBS_DOCS_TABLE_SCHEMA}]},
        resources=[[
            {"source": "clearmash", "id": "1", "version": "5",
             "source_doc": '{"title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": true}',
             "title": json.dumps({"el":"greek title ελληνικά, elliniká"}), "title_he": "", "title_en": "",
             "content_html": '{"el":"greek content<br/><b>HTML!</b>"}', "content_html_en": "", "content_html_he": ""},
            {"source": "clearmash", "id": "2", "version": "5",
             "source_doc": '{"title": "222", "content": "2222", "implemented": "not yet", "sorry": true}',
             "title": "", "title_he": "", "title_en": "",
             "content_html": '{"it":"italian content<br/><b>HTML!</b>"}', "content_html_en": "", "content_html_he": ""}
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
    assert es.get(ELASTICSEARCH_TESTS_INDEX, id="clearmash_1").get("_source") == {"version": "5",
                                                                                  "source": "clearmash",
                                                                                  "source_id": "1",
                                                                                  "title_el": "greek title ελληνικά, elliniká",
                                                                                  "title_he": "",
                                                                                  "title_en": "",
                                                                                  "content_html_el": "greek content<br/><b>HTML!</b>",
                                                                                  "content_html_en": "",
                                                                                  "content_html_he": "",
                                                                                  # this is from the source_doc
                                                                                  # (title field was overridden and deleted)
                                                                                  "content": "bazbax",
                                                                                  "implemented": "not yet",
                                                                                  "sorry": True}
    assert es.get(ELASTICSEARCH_TESTS_INDEX, id="clearmash_2").get("_source") == {"version": "5",
                                                                                  "source": "clearmash",
                                                                                  "source_id": "2",
                                                                                  "title_he": "",
                                                                                  "title_en": "",
                                                                                  "content_html_it": "italian content<br/><b>HTML!</b>",
                                                                                  "content_html_en": "",
                                                                                  "content_html_he": "",
                                                                                  "content": "2222",
                                                                                  "implemented": "not yet",
                                                                                  "sorry": True}


def test_update():
    es = initialize_elasticsearch()
    # same version as the version from mock resources - so, no update will be done
    updated_doc_2 = {"version": "5", "source": "clearmash",
                     "source_id": "2",
                     "title_en": "hello world in hebrew",
                     "content_html_he": "שלום עולם",
                     "content": "2222", "implemented": "not yet",
                     "sorry": True}
    es.index(index=ELASTICSEARCH_TESTS_INDEX, doc_type="common",
             body=updated_doc_2,
             id="clearmash_2")
    es.indices.refresh()
    assert_mock_resources_sync([
        {"source": "clearmash", "id": "1", "version": "5", "sync_msg": "added to ES"},
        {"source": "clearmash", "id": "2", "version": "5", "sync_msg": "no update needed"}
    ])
    es.indices.refresh()
    # item is as we inserted to ES - not updated
    assert es.get("mojptests", id="clearmash_2").get("_source")["title_en"] == "hello world in hebrew"
    # now, set to a different version, and it will update
    updated_doc_2["version"] = "6"
    es.index(index=ELASTICSEARCH_TESTS_INDEX, doc_type="common",
             body=updated_doc_2,
             id="clearmash_2")
    es.indices.refresh()
    assert_mock_resources_sync([
        {"source": "clearmash", "id": "1", "version": "5", "sync_msg": "no update needed"},
        {"source": "clearmash", "id": "2", "version": "5", "sync_msg": 'updated doc in ES (old version = "6")'}
    ])
    es.indices.refresh()
    assert es.get("mojptests", id="clearmash_1").get("_source") == {"version": "5",
                                                                    "source": "clearmash",
                                                                    "source_id": "1",
                                                                    "title_el": "greek title ελληνικά, elliniká",
                                                                    "title_he": "",
                                                                    "title_en": "",
                                                                    "content_html_el": "greek content<br/><b>HTML!</b>",
                                                                    "content_html_en": "",
                                                                    "content_html_he": "",
                                                                    # this is from the source_doc
                                                                    # (title field was overridden and deleted)
                                                                    "content": "bazbax",
                                                                    "implemented": "not yet",
                                                                    "sorry": True}
    assert es.get("mojptests", id="clearmash_2").get("_source") == {"version": "5",
                                                                    "source": "clearmash",
                                                                    "source_id": "2",
                                                                    "title_he": "",
                                                                    "title_en": "",
                                                                    "content_html_it": "italian content<br/><b>HTML!</b>",
                                                                    "content_html_en": "",
                                                                    "content_html_he": "",
                                                                    "content": "2222",
                                                                    "implemented": "not yet",
                                                                    "sorry": True}
