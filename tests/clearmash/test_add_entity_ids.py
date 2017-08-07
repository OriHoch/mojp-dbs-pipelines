from datapackage_pipelines_mojp.clearmash.processors.add_entity_ids import Processor as AddEntityIdsProcessor
from tests.clearmash.mock_clearmash_api import MockClearmashApi
from tests.common import get_mock_settings, assert_conforms_to_schema


class MockAddEntityIdsProcessor(AddEntityIdsProcessor):

    def _get_clearmash_api(self):
        return MockClearmashApi()

if __name__ == "__main__":
    MockAddEntityIdsProcessor.main()


def test_clearmash_add_entity_ids():
    settings = get_mock_settings(OVERRIDE_CLEARMASH_COLLECTIONS="",
                                 CLEARMASH_MAX_RETRIES=0,
                                 CLEARMASH_RETRY_SLEEP_SECONDS=0)
    parameters = {"add-resource": "entity-ids"}
    datapackage = {"resources": []}
    resources = []
    datapackage, resources, stats = MockAddEntityIdsProcessor(parameters, datapackage, resources, settings).spew()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    assert len(resource) == 50
    assert_conforms_to_schema(AddEntityIdsProcessor._get_schema(), resource[0])
    assert resource[0] == {'collection': 'familyNames', 'item_id': 115306}
    assert resource[10] == {'collection': 'places', 'item_id': 115325}
    assert resource[20] == {'collection': 'movies', 'item_id': 115414}
    assert resource[30] == {'collection': 'personalities', 'item_id': 115318}
    assert resource[40] == {'collection': 'photoUnits', 'item_id': 115301}
