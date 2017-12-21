import logging, sys, os
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_mojp.clearmash.api import ClearmashApi
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS


CLI_MODE = len(sys.argv) > 1 and sys.argv[1] == '--cli'


if CLI_MODE:
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    logging.debug("CLI MODE!")
    parameters, datapackage, resources = {}, {}, []
else:
    parameters, datapackage, resources = ingest()


parameters = dict({"max-recursion-depth": int(os.environ.get("MAX_RECURSION_DEPTH", "-1"))}, **parameters)
aggregations = {"stats": {}}


api = ClearmashApi()


def get_folder_items(folder_id, folder, recursion_depth=0):
    doc = api.get_web_document_system_folder(folder_id)
    for item in doc["Items"]:
        yield {"folder_id": folder_id,
               "collection": folder.get("collection"),
               "item_id": item["Id"],
               "item_name": item["Name"],
               "item_is_published": item["IsPublished"],
               "item_is_searchable": item["IsSearchable"],
               "item_permissions_type": item["PermissionType"], }
    for folder in doc["Folders"]:
        if parameters["max-recursion-depth"] == -1 or recursion_depth < parameters["max-recursion-depth"]:
            yield from get_folder_items(folder["Id"], {"collection": "unknown"}, recursion_depth+1)
        else:
            logging.warning("Reached folders max recursion depth")


def get_resource():
    for folder_id, folder in CONTENT_FOLDERS.items():
        yield from get_folder_items(folder_id, folder)


if CLI_MODE:
    for item in get_resource():
        print(item)
else:
    spew(dict(datapackage, resources=[{PROP_STREAMING: True,
                                       "name": "entity-ids",
                                       "path": "entity-ids.csv",
                                       "schema": {"fields": [{"name": "folder_id", "type": "integer"},
                                                             {"name": "collection", "type": "string"},
                                                             {"name": "item_id", "type": "integer"},
                                                             {"name": "item_name", "type": "string"},
                                                             {"name": "item_is_published", "type": "boolean"},
                                                             {"name": "item_is_searchable", "type": "boolean"},
                                                             {"name": "item_permissions_type", "type": "integer"},]}}]),
         [get_resource()], aggregations["stats"])
