from datapackage_pipelines_mojp.clearmash.api import ClearmashApi, ClearmashMediaGalleries, ClearmashChildDocuments, ClearmashRelatedDocuments
from datapackage_pipelines_mojp import settings
import os, json


class MockClearmashMediaGalleries(ClearmashMediaGalleries):
    pass


class MockClearmashChildDocuments(ClearmashChildDocuments):
    pass


class MockClearmashRelatedDocuments(ClearmashRelatedDocuments):

    def get_clearmash_api(self):
        return MockClearmashApi()


class MockClearmashApi(ClearmashApi):

    @property
    def related_documents(self):
        return MockClearmashRelatedDocuments

    @property
    def child_documents(self):
        return MockClearmashChildDocuments

    @property
    def media_galleries(self):
        return MockClearmashMediaGalleries

    def __init__(self, token=None):
        if not settings.CLEARMASH_CLIENT_TOKEN:
            token = "INVALID TOKEN"
        super(MockClearmashApi, self).__init__(token)

    def _get_request_json(self, url, headers, post_data=None):
        if url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Documents/Get":
            filename = "WebContentManagement_Documents_Get_{}".format("_".join(map(str, post_data["entitiesIds"])))
        elif url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Document/Folder/Get":
            folder_id = post_data["FolderId"]
            if folder_id in [40, 42, 43, 45, 49, 51, 69, 82]:
                filename = "WebContentManagement_Documents_Folder_Get_{}_{}".format(folder_id, post_data["FolderType"])
            else:
                # there are many subfolders, we return empty response for all
                filename = "WebContentManagement_Documents_Folder_Get_ALL_SUBFOLDERS"
        elif url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Document/ByRelationField":
            filename = "WebContentManagement_Document_ByRelationField_{}_{}_{}".format(post_data["EntityId"],
                                                                                       post_data["FieldId"],
                                                                                       post_data["MaxNestingDepth"])
        elif url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/CommunityPage/Folder/Root":
            filename = "WebContentManagement_CommunityPage_Folder_Root"
        elif url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Document/Folder/Root":
            filename = "WebContentManagement_Document_Folder_Root"
        else:
            raise Exception("unknown url / post_data: {} / {}".format(url, post_data))
        filename = os.path.join(os.path.dirname(__file__), "mocks", filename+".json")
        if not os.path.exists(filename):
            with open(filename, "w") as f:
                try:
                    data = super(MockClearmashApi, self)._get_request_json(url, headers, post_data)
                except Exception as e:
                    data = {"exception": str(e)}
                if url == "https://bh.clearmash.com/API/V5/Services/WebContentManagement.svc/Document/Folder/Get":
                    assert set(data.keys()) == {"Folders", "Items"}
                    data["Items"] = data["Items"][:10]
                json.dump(data, f)
        with open(filename) as f:
            data = json.load(f)
            if isinstance(data, dict) and list(data.keys()) == ["exception"]:
                raise Exception(data["exception"])
            else:
                return data
