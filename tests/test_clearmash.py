import json
from datapackage_pipelines_mojp.clearmash.processors.download import (ClearmashDownloadProcessor,
                                                                      TABLE_SCHEMA as DOWNLOAD_TABLE_SCHEMA)
from datapackage_pipelines_mojp.clearmash.processors.convert import ClearmashConvertProcessor
from .common import assert_processor
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
import os
from itertools import cycle
from copy import deepcopy


class MockClearMashApi(ClearmashApi):

    def __init__(self):
        super(MockClearMashApi, self).__init__(token="FAKE INVALID TOKEN")

    def get_web_document_system_folder(self, folder_id):
        return {"Folders": [],
                "Items": [{"Id": (folder_id*100+i)} for i in range(1, 50)]}

    def get_documents(self, entity_ids):
        response = {}
        with open(os.path.join(os.path.dirname(__file__), "mocks",
                               "clearmash-api-documents-get-115325-115353-115365-115371-115388-265694.json")) as f:
            mock_response = json.load(f)
        response["ReferencedDatasourceItems"] = mock_response["ReferencedDatasourceItems"]
        response["Entities"] = []
        infinite_entities = cycle(mock_response["Entities"])
        for entity_id in entity_ids:
            folder_id = int(entity_id / 100)
            entity = deepcopy(next(infinite_entities))
            entity["Document"].update(Id="foobarbaz-{}".format(entity_id),
                                      TemplateReference={"ChangesetId": folder_id*3,
                                                         "TemplateId": "fake-template-{}".format(folder_id)})
            entity["Metadata"].update(Id=entity_id, Url="http://foo.bar.baz/{}".format(entity_id))
            response["Entities"].append(entity)
        return response

class MockClearmashDownloadProcessor(ClearmashDownloadProcessor):

    def _get_clearmash_api(self):
        return MockClearMashApi()

def test_download():
    resources = assert_processor(
        MockClearmashDownloadProcessor,
        parameters={},
        datapackage={"resources": []},
        resources=[],
        expected_datapackage={
            "resources": [{
                "name": "clearmash",
                "path": "clearmash.csv",
                "schema": DOWNLOAD_TABLE_SCHEMA
            }]
        }
    )
    resource = next(resources)
    first_docs = []

    for doc in resource:
        folder_id = int(doc["item_id"]/100)
        if doc["item_id"]%(folder_id*100) == 1:
            first_docs.append(doc)
    assert len(first_docs) == 5
    def remove_large_fields(doc):
        doc.pop("metadata")
        doc.pop("parsed_doc")
        return doc
    assert list(map(remove_large_fields, first_docs)) == [{'document_id': 'foobarbaz-4501',
                                                           'item_id': 4501,
                                                           'item_url': 'http://foo.bar.baz/4501',
                                                           'template_changeset_id': 135,
                                                           'template_id': 'fake-template-45'},
                                                          {'document_id': 'foobarbaz-4301',
                                                           'item_id': 4301,
                                                           'item_url': 'http://foo.bar.baz/4301',
                                                           'template_changeset_id': 129,
                                                           'template_id': 'fake-template-43'},
                                                          {'document_id': 'foobarbaz-4001',
                                                           'item_id': 4001,
                                                           'item_url': 'http://foo.bar.baz/4001',
                                                           'template_changeset_id': 120,
                                                           'template_id': 'fake-template-40'},
                                                          {'document_id': 'foobarbaz-4901',
                                                           'item_id': 4901,
                                                           'item_url': 'http://foo.bar.baz/4901',
                                                           'template_changeset_id': 147,
                                                           'template_id': 'fake-template-49'},
                                                          {'document_id': 'foobarbaz-4201',
                                                           'item_id': 4201,
                                                           'item_url': 'http://foo.bar.baz/4201',
                                                           'template_changeset_id': 126,
                                                           'template_id': 'fake-template-42'}]


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
