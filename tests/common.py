from jsontableschema.model import SchemaModel
from jsontableschema import Schema
from elasticsearch import Elasticsearch, NotFoundError
import json, logging, os
from datapackage_pipelines_mojp.common.constants import DBS_DOCS_TABLE_SCHEMA, PIPELINES_ES_DOC_TYPE
from copy import deepcopy
from datapackage_pipelines_mojp.common.db import get_session
from sqlalchemy import MetaData, Table, Column, Integer, DateTime

ELASTICSEARCH_TESTS_INDEX = os.environ.get("TESTS_MOJP_ELASTICSEARCH_INDEX", "mojptests")
ELASTICSEARCH_TESTS_HOST = os.environ.get("TESTS_MOJP_ELASTICSEARCH_DB", "localhost:9200")


def assert_conforms_to_dbs_schema(row):
    return assert_conforms_to_schema(DBS_DOCS_TABLE_SCHEMA, row)

def assert_conforms_to_schema(schema, doc):
    assert isinstance(doc, dict), "invalid doc: {}".format(doc)
    row = [doc[field["name"]] for field in schema["fields"]]
    try:
        Schema(schema).cast_row(row)
    except Exception as e:
        logging.exception(e)
        raise Exception("row does not conform to schema\nrow='{}'\nschema='{}'".format(json.dumps(row),
                                                                                       json.dumps(schema)))
    schema_model = SchemaModel(schema)
    res = {}
    for k, v in doc.items():
        try:
            res[k] = schema_model.cast(k, v)
        except Exception as e:
            logging.exception(e)
            raise Exception("doc attribute '{}' with value '{}' "
                            "does not conform to schema '{}'".format(*map(json.dumps, [k, v, schema])))
    return res

def assert_resource_conforms_to_schema(schema, resource, max_rows=10):
    for doc in resource[:max_rows]:
        assert_conforms_to_schema(schema, doc)

def given_empty_elasticsearch_instance(host=ELASTICSEARCH_TESTS_HOST, index=ELASTICSEARCH_TESTS_INDEX):
    es = Elasticsearch(host)
    try:
        es.indices.delete(index)
    except NotFoundError:
        pass
    # we create only a part of the full index - only what's needed for our testing purposes
    # to get the full index you should use dbs-back scripts/elasticsearch_create_index script
    es.indices.create(index, body={"mappings": {PIPELINES_ES_DOC_TYPE: {"properties": {"slugs": {"type": "keyword"}}}}})
    es.indices.refresh(index=index)
    return es

def es_doc(es, source, source_id):
    try:
        return es.get(ELASTICSEARCH_TESTS_INDEX, id="{}_{}".format(source, source_id)).get("_source")
    except NotFoundError:
        return None

def get_mock_settings(**kwargs):
    default_settings = {"MOJP_ELASTICSEARCH_DB": ELASTICSEARCH_TESTS_HOST,
                        "MOJP_ELASTICSEARCH_INDEX": ELASTICSEARCH_TESTS_INDEX}
    default_settings.update(kwargs)
    return type("MockSettings", (object,), default_settings)

def assert_processor(processor):
    datapackage, resources = processor.spew()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    for doc in resource:
        assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
    return resource

def assert_dict(actual, expected):
    actual = deepcopy(actual)
    expected = deepcopy(expected)
    keys = expected.pop("keys", None)
    actual_expected_items = {k: actual.get(k) for k in expected}
    for k, v in expected.items():
        assert actual.pop(k, None) == v, "actual = {}".format(actual_expected_items)
    if keys is not None:
        actual_expected_items = {k: actual.get(k) for k in keys}
        assert set(actual.keys()) == set(keys), "actual keys = {}, actual items = {}".format(set(actual.keys()), actual_expected_items)

def get_test_db_session():
    session = get_session(connection_string="sqlite://")
    metadata = MetaData(bind=session.connection())
    Table("clearmash-entities", metadata,
          Column("item_id", Integer),
          Column("last_synced", DateTime),
          Column("hours_to_next_sync", Integer)).create()
    return session
