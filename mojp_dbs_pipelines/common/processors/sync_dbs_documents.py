from mojp_dbs_pipelines.common.processors.base_processors import FilterResourcesProcessor

DBS_DOCS_TABLE_SCHEMA = {"fields": [{"name": "source", "type": "string"},
                                    {'name': 'id', 'type': 'string'},
                                    {"name": "doc", "type": "string"}]}

DBS_DOCS_SYNC_LOG_TABLE_SCHAME = {"fields": [{"name": "source", "type": "string"},
                                             {'name': 'id', 'type': 'string'},
                                             {"name": "sync_msg", "type": "string"}]}


class SyncDbsDocumentsProcessor(FilterResourcesProcessor):

    def _filter_resource_descriptor(self, descriptor):
        if descriptor["name"] == "dbs_docs":
            descriptor.update({"name": "dbs_docs_sync_log",
                               "path": "dbs_docs_sync_log.csv",
                               "schema": DBS_DOCS_SYNC_LOG_TABLE_SCHAME})
        return descriptor

    def _filter_row(self, row, resource_descriptor):
        if resource_descriptor["name"] == "dbs_docs_sync_log":
            row = {"source": row["source"],
                   "id": row["id"],
                   "sync_msg": "not implemented yet"}
        return row


if __name__ == '__main__':
    SyncDbsDocumentsProcessor.main()
