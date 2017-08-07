from datapackage_pipelines_mojp.clearmash.processors.download import Processor as ClearmashDownloadProcessor
from tests.clearmash.mock_clearmash_api import MockClearmashApi
from tests.common import assert_conforms_to_schema, get_mock_settings


class MockClearmashDownloadProcessor(ClearmashDownloadProcessor):

    def _get_clearmash_api(self):
        return MockClearmashApi()

if __name__ == "__main__":
    MockClearmashDownloadProcessor.main()


def assert_dict(actual, expected):
    keys = expected.pop("keys", None)
    try:
        for k, v in expected.items():
            assert actual.pop(k) == v
        if keys is not None:
            assert set(actual.keys()) == set(keys)
    except Exception:
        for k, v in expected.items():
            print(k, actual.get(k))
        raise


def assert_downloaded_doc(actual, expected):
    assert_conforms_to_schema(ClearmashDownloadProcessor._get_schema(), actual)
    if "parsed_doc" in expected:
        assert_dict(actual.pop("parsed_doc"), expected.pop("parsed_doc"))
    if "metadata" in expected:
        assert_dict(actual.pop("metadata"), expected.pop("metadata"))
    assert_dict(actual, expected)


def run_download_processor(input_data=None):
    if not input_data:
        input_data = [{"item_id": 115306, "collection": "familyNames"},
                      {"item_id": 115325, "collection": "places"},
                      {"item_id": 115800, "collection": "movies"},
                      {"item_id": 115318, "collection": "personalities"},
                      {"item_id": 115301, "collection": "photoUnits"}]
    settings = get_mock_settings(OVERRIDE_CLEARMASH_ITEM_IDS="")
    parameters = {"input-resource": "entity-ids", "output-resource": "entities"}
    datapackage = {"resources": [{"name": "entity-ids"}]}
    resources = [input_data]
    return MockClearmashDownloadProcessor(parameters, datapackage, resources, settings).spew()

def get_downloaded_docs(input_data=None):
    if input_data:
        expected_len = None
    else:
        expected_len = 5
    datapackage, resources, stats = run_download_processor(input_data)
    assert len(datapackage["resources"]) == 1
    assert datapackage["resources"][0]["name"] == "entities"
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    if expected_len:
        assert len(resource) == expected_len
    return resource

def test_clearmash_download():
    resource = get_downloaded_docs()
    assert_downloaded_doc(resource[0], {"document_id": "f91ea044052746a2903d6ee60d9b374b",
                                        "item_id": 115306,
                                        "item_url": "http://bh.clearmash.com/skn/c6/dummy/e115306/dummy/",
                                        "template_changeset_id": 5135675,
                                        "template_id": "_c6_beit_hatfutsot_bh_family_name",
                                        "changeset": 6468918,
                                        "collection": "familyNames",
                                        "keys": ['hours_to_next_download', 'last_downloaded', 'last_synced', 'display_allowed'],
                                        "parsed_doc": {"entity_name": {'en': 'BEN AMARA', 'he': 'בן עמרה'},
                                                       "entity_id": 115306,
                                                       "entity_type_id": 1009,
                                                       "keys": ['entity_has_pending_changes', 'is_archived',
                                                                'is_deleted',
                                                                '_c6_beit_hatfutsot_bh_base_template_ugc',
                                                                '_c6_beit_hatfutsot_bh_base_template_old_numbers_parent',
                                                                '_c6_beit_hatfutsot_bh_base_template_display_status',
                                                                '_c6_beit_hatfutsot_bh_base_template_rights',
                                                                '_c6_beit_hatfutsot_bh_base_template_working_status',
                                                                'entity_creation_date',
                                                                'EntityFirstPublishDate',
                                                                'EntityLastPublishDate',
                                                                '_c6_beit_hatfutsot_bh_base_template_last_updated_date_bhp',
                                                                'community_id',
                                                                'EntityViewsCount',
                                                                '_c6_beit_hatfutsot_bh_base_template_description',
                                                                '_c6_beit_hatfutsot_bh_base_template_editor_remarks',
                                                                '_c6_beit_hatfutsot_bh_base_template_bhp_unit',
                                                                '_c6_beit_hatfutsot_bh_base_template_family_name',
                                                                '_c6_beit_hatfutsot_bh_base_template_multimedia_movies',
                                                                '_c6_beit_hatfutsot_bh_base_template_multimedia_music',
                                                                '_c6_beit_hatfutsot_bh_base_template_multimedia_photos',
                                                                '_c6_beit_hatfutsot_bh_base_template_related_exhibition',
                                                                '_c6_beit_hatfutsot_bh_base_template_related_musictext',
                                                                '_c6_beit_hatfutsot_bh_base_template_related_personality',
                                                                '_c6_beit_hatfutsot_bh_base_template_related_place',
                                                                '_c6_beit_hatfutsot_bh_base_template_related_recieve_unit',
                                                                '_c6_beit_hatfutsot_bh_base_template_source']},
                                        "metadata": {'ActiveLock': None,
                                                     'CreatorUserId': 2,
                                                     'EntityTypeId': 1009,
                                                     'IsArchived': False,
                                                     'IsBookmarked': False,
                                                     'IsLiked': False,
                                                     'LikesCount': 0,
                                                     "keys": []}})
    assert_downloaded_doc(resource[1], {"item_id": 115325, "template_id": "_c6_beit_hatfutsot_bh_place",
                                        "collection": "places",
                                        "parsed_doc": {"entity_name": {'en': 'Neuchatel', 'he': 'נשאטל'},
                                                       "entity_id": 115325, "entity_type_id": 1007}})
    assert_downloaded_doc(resource[2], {"item_id": 115800, "template_id": "_c6_beit_hatfutsot_bh_films",
                                        "collection": "movies",
                                        "parsed_doc": {"entity_name": {'en': 'Julius Axelrod,  Jewish Winner of the Nobel Prize (French)',
                                                                       'he': 'יוליוס אקסלרוד, חתן פרס נובל יהודי (צרפתית)'},
                                                       "entity_id": 115800, "entity_type_id": 1004}})
    assert_downloaded_doc(resource[3], {"item_id": 115318, "template_id": "_c6_beit_hatfutsot_bh_personality",
                                        "collection": "personalities",
                                        "parsed_doc": {"entity_name": {'en': 'Rein, Lotte', 'he': 'ריין, לוטה'},
                                                       "entity_id": 115318, "entity_type_id": 1013}})
    assert_downloaded_doc(resource[4], {"item_id": 115301, "template_id": "_c6_beit_hatfutsot_bh_photos",
                                        "collection": "photoUnits",
                                        "parsed_doc": {"entity_name": {'en': 'Cinderella, Performed by Jewish School Children, Mikulas, 1934',
                                                                       'he': 'סינדרלה, הוצג ע"י ילדי בית הספר היהודי, מיקולאש, צ\'כוסלובקיה, 1934'},
                                                       "entity_id": 115301, "entity_type_id": 1006}})
