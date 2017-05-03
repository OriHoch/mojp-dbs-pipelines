from .common import assert_processor
from mojp_dbs_pipelines.common.processors.sync_dbs_documents import SyncDbsDocumentsProcessor


def test_sync():
    assert_processor(
        SyncDbsDocumentsProcessor,
        parameters={},
        datapackage={
            'resources': [{
                'name': 'dbs_docs', 'path': 'dbs_docs.csv',
                'schema': {'fields': [{"name": "source", "type": "string"}, {'name': 'id', 'type': 'string'}]}
            }]
        },
        resources=[[
            {"source": "clearmash", "id": "1"},
            {"source": "clearmash", "id": "2"}
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
        expected_resources=[[
            {"source": "clearmash", "id": "1", "sync_msg": "not implemented yet"},
            {"source": "clearmash", "id": "2", "sync_msg": "not implemented yet"}
        ]]
    )
