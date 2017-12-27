import logging, sys, os, json
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, parse_clearmash_documents
from datapackage_pipelines_mojp.clearmash.constants import TEMPLATE_ID_COLLECTION_MAP
from datapackage_pipelines_mojp.clearmash.common import doc_show_filter


CLI_MODE = len(sys.argv) > 1 and sys.argv[1] == '--cli'


if CLI_MODE:
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    logging.debug("CLI MODE!")
    parameters, datapackage, resources = {}, {}, []
else:
    parameters, datapackage, resources = ingest()


DEFAULT_PARAMETERS = {}
parameters = dict(DEFAULT_PARAMETERS, **parameters)
stats = {"total downloaded": 0, "not allowed": 0, "allowed": 0}
aggregations = {"stats": stats}


api = ClearmashApi()


def get_entity(entity_id_row):
    for doc in parse_clearmash_documents(api.get_documents([entity_id_row["item_id"]])):
        doc.update(collection=TEMPLATE_ID_COLLECTION_MAP.get(doc["template_id"], "unknown"),
                   display_allowed=doc_show_filter(doc["parsed_doc"]))
        stats["total downloaded"] += 1
        if doc["display_allowed"]:
            stats["allowed"] += 1
        else:
            stats["not allowed"] += 1
        yield doc


def get_resource():
    for resource in resources:
        for row in resource:
            if not row["folder_id"] and not row["collection"] and not row["item_id"]:
                assert json.loads(row["item_name"]).get("reached max recursion depth")
            else:
                yield get_entity(row)


if CLI_MODE:
    raise NotImplemented()
else:
    spew(dict(datapackage, resources=[{PROP_STREAMING: True,
                                       "name": "entities",
                                       "path": "entities.csv",
                                       "schema": {"fields": [{"name": "document_id", "type": "string",
                                                              "description": "some sort of internal GUID"},
                                                             {"name": "item_id", "type": "integer",
                                                              "description": "the item id as requested from the folder"},
                                                             {"name": "item_url", "type": "string",
                                                              "description": "url to view the item in CM"},
                                                             {"name": "collection", "type": "string",
                                                              "description": "common dbs docs collection string"},
                                                             {"name": "template_changeset_id", "type": "integer",
                                                              "description": "I guess it's the id of template when doc was saved"},
                                                             {"name": "template_id", "type": "string",
                                                              "description": "can help to identify the item type"},
                                                             {"name": "changeset", "type": "integer",
                                                              "description": ""},
                                                             {"name": "metadata", "type": "object",
                                                              "description": "full metadata"},
                                                             {"name": "parsed_doc", "type": "object",
                                                              "description": "all other attributes"},
                                                             {"name": "display_allowed", "type": "boolean"}],
                                                  "primaryKey": ["item_id"]}}]),
         [get_resource()], aggregations["stats"])
