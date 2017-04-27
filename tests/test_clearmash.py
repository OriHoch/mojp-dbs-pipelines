from .common import listify_resources
from mojp_dbs_pipelines.clearmash.processors.add_resources import AddClearmashResources
from mojp_dbs_pipelines.clearmash.processors.convert_to_dbs_documents import ConvertClearmashResources
import json


def test_download():
    # no input - new datapackage
    parameters = {}
    datapackage = {"resources": []}
    resources = []
    datapackage, resources = AddClearmashResources(parameters, datapackage, resources).spew()
    # added a "clearmash" resource with documents in clearmash schema
    assert datapackage == {"resources": [{"name": "clearmash",
                                          "path": "clearmash.csv",
                                          "schema": {"fields": [{"name": "id", "type": "integer"},
                                                                {"name": "doc", "type": "string"}]}}]}
    assert listify_resources(resources) == [[{"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})},
                                             {"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})}]]


def test_convert():
    # datapackage with the clearmash documents
    parameters = {}
    datapackage = {"resources": [{"name": "clearmash",
                                          "path": "clearmash.csv",
                                          "schema": {"fields": [{"name": "id", "type": "integer"},
                                                                {"name": "doc", "type": "string"}]}}]}
    resources = [[{"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})},
                  {"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})}]]
    datapackage, resources = ConvertClearmashResources(parameters, datapackage, resources).spew()
    # remove the clearmash resource and added a dbs_docs resource with docs according to common schema for all sources
    assert datapackage == {"resources": [{"name": "dbs_docs",
                                          "path": "dbs_docs.csv",
                                          "schema": {"fields": [{"name": "source", "type": "string"},
                                                                {"name": "id", "type": "string"},
                                                                {"name": "doc", "type": "string"}]}}]}
    assert listify_resources(resources) == [[{"source": "clearmash", "id": "1", "doc": '{"title": "foobar", "content": "bazbax", "implemented": "not yet", "sorry": true}'},
                                             {"source": "clearmash", "id": "2", "doc": '{"title": "222", "content": "2222", "implemented": "not yet", "sorry": true}'}]]
