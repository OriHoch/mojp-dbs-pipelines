# /mojp-dbs-pipelines/tests/bagnowka/test_convert.py
from datapackage_pipelines.wrapper import spew
from datapackage_pipelines_mojp.bagnowka.processors.convert import Processor
from datapackage_pipelines_mojp.bagnowka.processors.download import Processor as BagnowkaDownloadProcessor
from ..common import assert_conforms_to_schema, get_mock_settings, assert_processor, assert_dict
import os
import json
# from .test_download import get_downloaded_docs

def test_bagnowka_convert():
    settings = get_mock_settings()
    parameters = {"add-resource": "bagnowka"}
    datapackage = {"resources": [{"name": "bagnowka"}]}
    all_docs = open(os.path.join(os.path.dirname(__file__), "bagnowka_10.json"))
    resources = json.load(all_docs)
    resource = BagnowkaDownloadProcessor(parameters, datapackage, resources).spew()

    convert_params = {"input-resource": "bagnowka", 
                    "output-resource": "dbs-docs"}
    convert_datapackage = {"resources": [{"name": "bagnowka"}]}

    datapackage, resources = Processor(convert_params, convert_datapackage, resource).spew()
    resources = list(resources)
    assert len(resources) == 1
    res = list(resources[0])
    assert len(res) == 3
    # for doc in res:
    #     assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
    # assert len(res) == 3

    #assert len(docs) == 5
#     return docs


# def assert_processor(processor, resources):
#     datapackage, resources = processor.spew()
#     resources = list(resources)
#     assert len(resources) == 1
#     resource = list(resources[0])
#     for doc in resource:
#         assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
#     return resource


#    assert len(docs) == 5
#     assert len(resource[0].pop("content_html_he")) == 1060
#     assert len(resource[0].pop("content_html_en")) == 1399
#     assert_dict(resource[0], {"collection": "familyNames",
#                               "content_html": {},
#                               "title_en": "BEN AMARA",
#                               "title_he": "בן עמרה",
#                               "title": {},
#                               "id": "115306",
#                               "source": "clearmash",
#                               "version": "6468918-f91ea044052746a2903d6ee60d9b374b",
#                               "related_documents": {'_c6_beit_hatfutsot_bh_base_template_family_name': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_movies': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_music': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_photos': [], '_c6_beit_hatfutsot_bh_base_template_related_exhibition': [], '_c6_beit_hatfutsot_bh_base_template_related_musictext': [], '_c6_beit_hatfutsot_bh_base_template_related_personality': [], '_c6_beit_hatfutsot_bh_base_template_related_place': [], '_c6_beit_hatfutsot_bh_base_template_related_recieve_unit': [], '_c6_beit_hatfutsot_bh_base_template_source': []},
#                               "main_image_url": "",
#                               "main_thumbnail_image_url": "",
#                               "keys": ['source_doc']})
#     assert_dict(resource[1], {"collection": "places"})
#     assert_dict(resource[2], {"collection": "movies"})
#     assert_dict(resource[3], {"collection": "personalities"})
#     assert_dict(resource[4], {"collection": "photoUnits"})





# def get_downloaded_docs():
#     return BagnowkaDownloadProcessor()

# def get_bagnowka_convert_resource_data():
#     processor = Processor(parameters={"input-resource": "bagnowka", "output-resource": "dbs-docs"},
#                               datapackage={"resources": [{"name": "bagnowka"}]},
#                               resources=[get_downloaded_docs()],
#                               settings=get_mock_settings())
#     docs = assert_processor(processor)
#     assert len(docs) == 5
#     return docs


# def assert_processor(processor):
#     datapackage, resources = processor.spew()
#     resources = list(resources)
#     assert len(resources) == 1
#     resource = list(resources[0])
#     for doc in resource:
#         assert_conforms_to_schema(datapackage["resources"][0]["schema"], doc)
#     return resource







# def test_clearmash_convert():
#     resource = get_clearmash_convert_resource_data()
#     assert len(resource) == 5
#     assert len(resource[0].pop("content_html_he")) == 1060
#     assert len(resource[0].pop("content_html_en")) == 1399
#     assert_dict(resource[0], {"collection": "familyNames",
#                               "content_html": {},
#                               "title_en": "BEN AMARA",
#                               "title_he": "בן עמרה",
#                               "title": {},
#                               "id": "115306",
#                               "source": "clearmash",
#                               "version": "6468918-f91ea044052746a2903d6ee60d9b374b",
#                               "related_documents": {'_c6_beit_hatfutsot_bh_base_template_family_name': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_movies': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_music': [], '_c6_beit_hatfutsot_bh_base_template_multimedia_photos': [], '_c6_beit_hatfutsot_bh_base_template_related_exhibition': [], '_c6_beit_hatfutsot_bh_base_template_related_musictext': [], '_c6_beit_hatfutsot_bh_base_template_related_personality': [], '_c6_beit_hatfutsot_bh_base_template_related_place': [], '_c6_beit_hatfutsot_bh_base_template_related_recieve_unit': [], '_c6_beit_hatfutsot_bh_base_template_source': []},
#                               "main_image_url": "",
#                               "main_thumbnail_image_url": "",
#                               "keys": ['source_doc']})
#     assert_dict(resource[1], {"collection": "places"})
#     assert_dict(resource[2], {"collection": "movies"})
#     assert_dict(resource[3], {"collection": "personalities"})
#     assert_dict(resource[4], {"collection": "photoUnits"})
