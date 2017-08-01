# /mojp-dbs-pipelines/tests/bagnowka/test_convert.py
from datapackage_pipelines.wrapper import spew
from datapackage_pipelines_mojp.bagnowka.processors.download import BagnowkaDownloadProcessor
from datapackage_pipelines_mojp.bagnowka.processors.convert import BagnowkaConvertProcessor
# from datapackage_pipelines_mojp.bagnowka.processors.download import BagnowkaDownloadProcessor
from ..common import assert_conforms_to_schema, get_mock_settings, assert_processor, assert_dict
from .test_download import MockBagnowkaDownloadProcessor, test_bagnowka_download
import os
import json


def get_bagnowka_convert_resource_data():
    d_parameters = {"add-resource": "bagnowka"}
    d_datapackage = {"resources": []}
    d_resources = []
    processor = BagnowkaConvertProcessor(parameters={"input-resource": "bagnowka", "output-resource": "dbs-docs"},
                              datapackage={"resources": [
                                  {"name": "bagnowka"}]},
                              resources=[MockBagnowkaDownloadProcessor(
                                  d_parameters, d_datapackage, d_resources)._get_resource()],
                              settings=get_mock_settings())
    docs = assert_processor(processor)
    assert len(docs) == 10
    return docs


def test_converted_docs():
    docs = get_bagnowka_convert_resource_data()
    assert len(docs) == 10
    assert_dict(docs[0], {'collection': 'photoUnits', 'content_html': {'content_en': 'Today: Poland, in 1918 - 1939 Poland. Pre 1914 Germany, pre 1795 Poland.\nCourtesy of www.bagnowka.pl', 'content_he': ''}, 'title_en': 'Tuchola, 1910', 'title_he': '', 'title': {'title_en': 'Tuchola, 1910'}, 'id': '11111111112901', 'source': 'Bagnowka', 'version': 'one', 'related_documents': {}, 'main_image_url': '', 'main_thumbnail_image_url': ''})
    assert docs[1]["id"] == "11111111117135"
    assert docs[5]["title_en"] == "Grajewo - the people, soldiers, 1916"

def assert_processor(processor):
    datapackage, resources = processor.spew()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    for doc in resource:
        assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
    return resource