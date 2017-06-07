import json
from datapackage_pipelines_mojp.common.processors.base_processors import AddResourcesProcessor
from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, parse_clearmash_document
from datapackage_pipelines_mojp.clearmash.constants import (WEB_CONTENT_FOLDER_ID_FamilyName,
                                                            WEB_CONTENT_FOLDER_ID_Place,
                                                            WEB_CONTENT_FOLDER_ID_Movies,
                                                            WEB_CONTENT_FOLDER_ID_Personality,
                                                            WEB_CONTENT_FOLDER_ID_Photos)

TABLE_SCHEMA = {"fields": [{"name": "document_id", "type": "string",
                            "description": "some sort of internal GUID"},
                           {"name": "item_id", "type": "integer",
                            "description": "the item id as requested from the folder"},
                           {"name": "item_url", "type": "string",
                            "description": "url to view the item in CM"},
                           {"name": "template_changeset_id", "type": "integer",
                            "description": "I guess it's the id of template when doc was saved"},
                           {"name": "template_id", "type": "string",
                            "description": "can help to identify the item type"},

                           {"name": "metadata", "type": "string",
                            "description": "json of remaining unparsed attribute"},
                           {"name": "parsed_doc", "type": "string",
                            "description": "json of remaining unparsed attribute"}]}


CONTENT_FOLDERS = {
    WEB_CONTENT_FOLDER_ID_FamilyName: {},
    WEB_CONTENT_FOLDER_ID_Place: {},
    WEB_CONTENT_FOLDER_ID_Movies: {},
    WEB_CONTENT_FOLDER_ID_Personality: {},
    WEB_CONTENT_FOLDER_ID_Photos: {},
}

ITEM_IDS_BUFFER_LENGTH = 10


class ClearmashDownloadProcessor(AddResourcesProcessor):

    def __init__(self, *args, **kwargs):
        self.clearmash_api = self._get_clearmash_api()
        super(ClearmashDownloadProcessor, self).__init__(*args, **kwargs)

    def _get_clearmash_api(self):
        return ClearmashApi()

    def _get_resource_descriptors(self):
        return [{"name": "clearmash",
                 "path": "clearmash.csv",
                 "schema": TABLE_SCHEMA}]

    def _download(self):
        for folder_id, folder in CONTENT_FOLDERS.items():
            self.item_ids_buffer = []
            for item in self.clearmash_api.get_web_document_system_folder(folder_id)["Items"]:
                self.item_ids_buffer.append(item["Id"])
                yield from self._handle_item_ids_buffer(folder_id, folder)
            yield from self._handle_item_ids_buffer(folder_id, folder, force_flush=True)

    def _handle_item_ids_buffer(self, folder_id, folder, force_flush=False):
        if force_flush or len(self.item_ids_buffer) > ITEM_IDS_BUFFER_LENGTH:
            documents_response = self.clearmash_api.get_documents(self.item_ids_buffer)
            reference_datasource_items = documents_response.pop("ReferencedDatasourceItems")
            entities = documents_response.pop("Entities")
            for entity in entities:
                document = entity.pop("Document")
                metadata = entity.pop("Metadata")
                template_reference = document.pop("TemplateReference")
                document_id = document.pop("Id")
                parsed_doc = parse_clearmash_document(document, reference_datasource_items)
                yield {"document_id": document_id,
                       "item_id": metadata.pop("Id"),
                       "item_url": metadata.pop("Url"),
                       "template_changeset_id": template_reference.pop("ChangesetId"),
                       "template_id": template_reference.pop("TemplateId"),
                       "metadata": json.dumps(metadata),
                       "parsed_doc": json.dumps(parsed_doc),
                       "changeset": entity.pop("Changeset")}
            self.item_ids_buffer = []

    def _get_resources_iterator(self):
        return [self._download()]


if __name__ == '__main__':
    ClearmashDownloadProcessor.main()
