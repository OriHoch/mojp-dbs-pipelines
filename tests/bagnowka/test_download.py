# /mojp-dbs-pipelines/tests/bagnowka/test_download.py

from datapackage_pipelines_mojp.bagnowka.processors.download import BagnowkaDownloadProcessor
from tests.common import assert_conforms_to_schema, get_mock_settings
import os
import json
import logging


class MockBagnowkaDownloadProcessor(BagnowkaDownloadProcessor):

    def __init__(self, *args, **kwargs):
        super(BagnowkaDownloadProcessor, self).__init__(*args, **kwargs)

    def _get_resource(self):
        with open(os.path.join(os.path.dirname(__file__), "bagnowka_10.json")) as f:
            all_docs = json.load(f)
            for item_data in all_docs:
                new = all_docs[item_data]
                doc = self.download(new)
                logging.info("hello world")
                yield doc


if __name__ == "__main__":
    MockBagnowkaDownloadProcessor.main()


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
    assert_conforms_to_schema(
        MockBagnowkaDownloadProcessor._get_schema(), actual)
    assert_dict(actual, expected)


def test_bagnowka_download():
    parameters = {"add-resource": "bagnowka"}
    datapackage = {"resources": []}
    resources = []
    datapackage, resources = MockBagnowkaDownloadProcessor(
        parameters, datapackage, resources).spew()
    return resources
    assert len(datapackage["resources"]) == 1
    assert datapackage["resources"][0]["name"] == "bagnowka"
    resources = list(resources)
    resource = list(resources[0])
    assert len(resource) == 10
    assert resource[2] == {'approximate_date_taken': '1920',
                           'desc': 'Today: Ukraine, in 1918 - 1939 Poland. Pre 1914 Russia.\n'
                           'Courtesy of www.bagnowka.pl. Photographer: Tomek',
                           'id': '11111111117173',
                           'main_image_url': 'https://s3-us-west-2.amazonaws.com/bagnowka-scraped/full/01431b26cf701957daf76c67fc32f216766fd12a.jpg',
                           'name': 'Krzemieniec - churches, Orthodox church, former catholic (unit?), '
                           '1920',
                           'pictures': {'picture_ids': ['01431b26cf701957daf76c67fc32f216766fd12a']}}
    assert_downloaded_doc(resource[1], {"id": "11111111117135"})
    assert_downloaded_doc(resource[5], {"pictures": {'picture_ids': ['f2a27ed9bb8ac15fe8f3db34461038eb2a44e9ba',
                                                                     'c088bdbbc9040ea78d477694b50d70efee8cbf84',
                                                                     '8e282972fd8fa1726916d5e3e80b4ebea6e66a6f',
                                                                     'e4bc91145bb3ed60de6471b4717f283bfe2075f6',
                                                                     'e47faa4c29316424fc272588b89c6ecd23cd2c3c']}})
