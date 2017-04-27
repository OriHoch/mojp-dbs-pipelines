from .common import listify_resources
from mojp_dbs_pipelines.common.processors.sync_dbs_documents import SyncDbsDocumentsProcessor


def test_sync():
    # datapackage with the clearmash documents
    parameters = {}
    datapackage = {'resources': [{'name': 'dbs_docs',
                                  'path': 'dbs_docs.csv',
                                  'schema': {'fields': [{"name": "source", "type": "string"},
                                                        {'name': 'id', 'type': 'string'}]}}]}
    resources = [[{"source": "clearmash", "id": "1"},
                  {"source": "clearmash", "id": "2"}]]
    datapackage, resources = SyncDbsDocumentsProcessor(parameters, datapackage, resources).spew()
    # remove the clearmash resource and added a dbs_docs resource with docs according to common schema for all sources
    assert datapackage == {'resources': [{'name': 'dbs_docs_sync_log',
                                          'path': 'dbs_docs_sync_log.csv',
                                          'schema': {'fields': [{"name": "source", "type": "string"},
                                                                {'name': 'id', 'type': 'string'},
                                                                {"name": "sync_msg", "type": "string"}]}}]}
    assert listify_resources(resources) == [[{"source": "clearmash", "id": "1", "sync_msg": "not implemented yet"},
                                             {"source": "clearmash", "id": "2", "sync_msg": "not implemented yet"}]]

