from mojp_dbs_pipelines.common.processors.base_processors import FilterResourcesProcessor
from elasticsearch import Elasticsearch, NotFoundError
import json

DBS_DOCS_TABLE_SCHEMA = {"fields": [
    {"name": "source", "type": "string"},
    {'name': 'id', 'type': 'string'},
    {"name": "version", "type": "string",
     "description": "source dependant field, used by sync process to detect document updates"},
    {"name": "source_doc", "type": "string"}
]}

DBS_DOCS_SYNC_LOG_TABLE_SCHAME = {"fields": [{"name": "source", "type": "string"},
                                             {'name': 'id', 'type': 'string'},
                                             {"name": "sync_msg", "type": "string"}]}


class CommonSyncProcessor(FilterResourcesProcessor):

    def __init__(self, *args, **kwargs):
        super(CommonSyncProcessor, self).__init__(*args, **kwargs)
        self._es = Elasticsearch(self._get_settings("MOJP_ELASTICSEARCH_DB"))
        self._idx = self._get_settings("MOJP_ELASTICSEARCH_INDEX")

    def _filter_resource_descriptor(self, descriptor):
        if descriptor["name"] == "dbs_docs":
            descriptor.update({"name": "dbs_docs_sync_log",
                               "path": "dbs_docs_sync_log.csv",
                               "schema": DBS_DOCS_SYNC_LOG_TABLE_SCHAME})
        return descriptor

    def _add_doc(self, source, source_id, source_doc, source_version):
        self._es.index(index=self._idx, doc_type="common", body=source_doc, id="{}_{}".format(source, source_id))
        return {"source": source, "id": source_id, "version": source_version, "sync_msg": "added to ES"}

    def _update_doc(self, source, source_id, source_doc, source_version, old_doc):
        old_version = old_doc["version"]
        if source_version != old_version:
            self._es.update(index=self._idx, doc_type="common",
                            id="{}_{}".format(source, source_id),
                            body={"doc": source_doc})
            return {"source": source, "id": source_id, "version": source_version,
                    "sync_msg": "updated doc in ES (old version = {})".format(json.dumps(old_version))}
        else:
            return {"source": source, "id": source_id, "version": source_version, "sync_msg": "no update needed"}

    def _filter_row(self, row, resource_descriptor):
        if resource_descriptor["name"] == "dbs_docs_sync_log":
            source = row["source"]
            source_id = row["id"]
            source_doc = json.loads(row["source_doc"])
            source_version = row["version"]
            source_doc["version"] = source_version
            try:
                doc = self._es.get(index=self._idx, id="{}_{}".format(source, source_id))
            except NotFoundError:
                doc = None
            if doc:
                return self._update_doc(source, source_id, source_doc, source_version, doc["_source"])
            else:
                return self._add_doc(source, source_id, source_doc, source_version)
        return row


if __name__ == '__main__':
    CommonSyncProcessor.main()
