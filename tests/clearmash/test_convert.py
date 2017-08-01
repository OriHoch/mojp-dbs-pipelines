from datapackage_pipelines_mojp.clearmash.processors.convert import Processor
from .mock_clearmash_api import MockClearmashApi
from ..common import get_mock_settings, assert_processor, assert_dict
from .test_download import get_downloaded_docs


class MockProcessor(Processor):

    def _get_clearmash_api(self):
        return MockClearmashApi()

def get_clearmash_convert_resource_data():
    processor = MockProcessor(parameters={"input-resource": "entities", "output-resource": "dbs-docs"},
                              datapackage={"resources": [{"name": "entities"}]},
                              resources=[get_downloaded_docs()],
                              settings=get_mock_settings())
    docs = assert_processor(processor)
    assert len(docs) == 5
    return docs


def test_clearmash_convert():
    resource = get_clearmash_convert_resource_data()
    assert len(resource) == 5
    assert len(resource[0].pop("content_html_he")) == 1060
    assert len(resource[0].pop("content_html_en")) == 1399
    assert_dict(resource[0], {"collection": "familyNames",
                              "content_html": {},
                              "title_en": "BEN AMARA",
                              "title_he": "בן עמרה",
                              "title": {},
                              "id": "115306",
                              "source": "clearmash",
                              "version": "6468918-f91ea044052746a2903d6ee60d9b374b",
                              "related_documents": {},
                              "main_image_url": "",
                              "main_thumbnail_image_url": "",
                              "keys": ['source_doc']})
    assert_dict(resource[1], {"collection": "places"})
    assert_dict(resource[2], {"collection": "movies"})
    assert_dict(resource[3], {"collection": "personalities"})
    assert_dict(resource[4], {"collection": "photoUnits"})


def test_clearmash_convert_images():
    resource = get_clearmash_convert_resource_data()
    photo = resource[4]
    assert photo["id"] == "115301"
    assert photo["main_image_url"] == "https://bhfiles.clearmash.com/MediaServer/Images/5ff94861dad3480c9e59f1904a825caf_1024x0.JPG"
    assert photo["main_thumbnail_image_url"] == "https://bhfiles.clearmash.com/MediaServer/Images/5ff94861dad3480c9e59f1904a825caf_260x0.JPG"
