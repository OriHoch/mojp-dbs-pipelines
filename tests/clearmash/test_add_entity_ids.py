from datapackage_pipelines_mojp.clearmash.processors.add_entity_ids import Processor as AddEntityIdsProcessor
from tests.clearmash.mock_clearmash_api import MockClearmashApi
from tests.common import get_mock_settings, assert_conforms_to_schema


class MockAddEntityIdsProcessor(AddEntityIdsProcessor):

    def _get_clearmash_api_class(self):
        return MockClearmashApi


if __name__ == "__main__":
    MockAddEntityIdsProcessor.main()


def test_clearmash_add_entity_ids():
    settings = get_mock_settings(OVERRIDE_CLEARMASH_COLLECTIONS="",
                                 CLEARMASH_MAX_RETRIES=0,
                                 CLEARMASH_RETRY_SLEEP_SECONDS=0)
    parameters = {"add-resource": "entity-ids", "folders-table": "clearmash-folders"}
    datapackage = {"resources": []}
    resources = []
    datapackage, resources, stats = MockAddEntityIdsProcessor(parameters, datapackage, resources, settings).spew()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    assert len(resource) == 50
    assert_conforms_to_schema(AddEntityIdsProcessor._get_schema(), resource[0])
    assert resource[0] == {'collection': 'familyNames', 'folder_id': 45,
                           'item_id': 115306,
                           'name': 'בן עמרה',
                           'metadata': {'CommunityId': 6,
                                        'CreatorPersonId': 2,
                                        'FileType': 0,
                                        'Id': 115306,
                                        'IsBookmarked': False,
                                        'IsFolder': False,
                                        'IsLiked': False,
                                        'IsPublished': True,
                                        'IsReadOnly': False,
                                        'IsSearchable': True,
                                        'LikesCount': 0,
                                        'LockedByPersonId': 0,
                                        'LockedByPersonName': '',
                                        'ModifiedByPersonId': 2,
                                        'ModifiedByPersonName': 'Admin ',
                                        'Name': 'בן עמרה',
                                        'ParentFolderId': -1,
                                        'PermissionType': 5,
                                        'SizeInBytes': 0,
                                        'UserCanPublish': True}}
    assert {k: resource[10][k] for k in ["collection", "item_id"]} == {'collection': 'places', 'item_id': 115325}
    assert {k: resource[20][k] for k in ["collection", "item_id"]} == {'collection': 'movies', 'item_id': 115414}
    assert {k: resource[30][k] for k in ["collection", "item_id"]} == {'collection': 'personalities', 'item_id': 115318}
    assert {k: resource[40][k] for k in ["collection", "item_id"]} == {'collection': 'photoUnits', 'item_id': 115301}
