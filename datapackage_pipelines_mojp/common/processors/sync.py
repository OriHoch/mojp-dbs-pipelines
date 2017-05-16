import json
from elasticsearch import Elasticsearch, NotFoundError
from datapackage_pipelines_mojp.common.processors.base_processors import FilterResourcesProcessor
import iso639


DBS_DOCS_TABLE_SCHEMA = {"fields": [{"name": "source", "type": "string"},
                                    {'name': 'id', 'type': 'string'},
                                    {"name": "version", "type": "string",
                                     "description": "source dependant field, used by sync process to detect document updates"},
                                    {"name": "source_doc", "type": "string"},
                                    {"name": "title", "type": "string"},
                                    {"name": "title_he", "type": "string"},
                                    {"name": "title_en", "type": "string"},
                                    {"name": "content_html", "type": "string"},
                                    {"name": "content_html_he", "type": "string"},
                                    {"name": "content_html_en", "type": "string"}]}

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

    def _add_doc(self, new_doc):
        self._es.index(index=self._idx, doc_type="common", body=new_doc,
                       id="{}_{}".format(new_doc["source"], new_doc["source_id"]))
        return {"source": new_doc["source"], "id": new_doc["source_id"], "version": new_doc["version"],
                "sync_msg": "added to ES"}

    def _update_doc(self, new_doc, old_doc):
        if old_doc["version"] != new_doc["version"]:
            self._es.update(index=self._idx, doc_type="common",
                            id="{}_{}".format(new_doc["source"], new_doc["source_id"]),
                            body={"doc": new_doc})
            return {"source": new_doc["source"], "id": new_doc["source_id"], "version": new_doc["version"],
                    "sync_msg": "updated doc in ES (old version = {})".format(json.dumps(old_doc["version"]))}
        else:
            return {"source": new_doc["source"], "id": new_doc["source_id"], "version": new_doc["version"], "sync_msg": "no update needed"}

    def _filter_row(self, row, resource_descriptor):
        if resource_descriptor["name"] == "dbs_docs_sync_log":
            source_doc = json.loads(row.pop("source_doc"))
            # base for the final es doc is the source doc
            new_doc = source_doc
            # then, we override with the other row values
            new_doc.update(row)
            # rename the id field
            new_doc["source_id"] = new_doc.pop("id")
            # populate the language fields
            for lang_field in ["title", "content_html"]:
                if row[lang_field]:
                    for lang, value in json.loads(row[lang_field]).items():
                        new_doc["{}_{}".format(lang_field, lang)] = value
                # delete the combined json lang field from the new_doc
                del new_doc[lang_field]
            try:
                old_doc = self._es.get(index=self._idx, id="{}_{}".format(new_doc["source"], new_doc["source_id"]))["_source"]
            except NotFoundError:
                old_doc = None
            if old_doc:
                return self._update_doc(new_doc, old_doc)
            else:
                return self._add_doc(new_doc)
        return row


if __name__ == '__main__':
    CommonSyncProcessor.main()
