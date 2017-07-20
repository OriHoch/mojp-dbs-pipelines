from datapackage_pipelines_mojp.common.processors.sync import CommonSyncProcessor
from .clearmash.test_convert import get_clearmash_convert_resource_data
from .common import (assert_processor, get_mock_settings, assert_dict, given_empty_elasticsearch_instance,
                     es_doc)

def assert_sync_processor(input_data=None):
    if not input_data:
        input_data = get_clearmash_convert_resource_data()
    processor = CommonSyncProcessor(parameters={"input-resource": "entities", "output-resource": "dbs-docs"},
                                    datapackage={"resources": [{"name": "entities"}]},
                                    resources=[input_data],
                                    settings=get_mock_settings())
    return assert_processor(processor)

def assert_item_missing_content(content_html_en, content_html_he, is_synced):
    given_empty_elasticsearch_instance()
    item = get_clearmash_convert_resource_data()[0]
    if content_html_en == "del":
        del item["content_html_en"]
    else:
        item["content_html_en"] = content_html_en
    if content_html_he == "del":
        del item["content_html_he"]
    else:
        item["content_html_he"] = content_html_he
    docs = assert_sync_processor([item])
    if is_synced:
        assert len(docs) == 1
    else:
        assert len(docs) == 0

def test_sync():
    es = given_empty_elasticsearch_instance()
    docs = assert_sync_processor()
    assert len(docs) == 5
    assert_dict(docs[0], {'id': '115306',
                          'version': '6468918-f91ea044052746a2903d6ee60d9b374b',
                          'sync_msg': 'added to ES',
                          'source': 'clearmash',
                          'collection': 'familyNames',
                          'keys': []})
    assert_dict(docs[1], {'id': '115325',
                          'version': '6468918-65e99e43a7164999971d8336a53f335e',
                          'sync_msg': 'added to ES',
                          'source': 'clearmash',
                          'collection': 'places',
                          'keys': []})
    assert_dict(docs[2], {'id': '115800', 'version': '6468918-460fe9869c46412db95daa136634be57', 'sync_msg': 'added to ES', 'source': 'clearmash', 'collection': 'movies', 'keys': []})
    assert_dict(docs[3], {'id': '115318', 'version': '6468918-ec135e2eb4a54022a4ece9e2ce4f46c4', 'sync_msg': 'added to ES', 'source': 'clearmash', 'collection': 'personalities', 'keys': []})
    assert_dict(docs[4], {'id': '115301', 'version': '6468918-6de9cf72a9ae4cc690a6c2437d21e0a6', 'sync_msg': 'added to ES', 'source': 'clearmash', 'collection': 'photoUnits', 'keys': []})
    es_docs = [es_doc(es, "clearmash", id) for id in ["115306", "115325", "115800", "115318", "115301"]]
    assert len(es_docs) == 5
    assert_dict(es_docs[0], {'slug_he': 'שםמשפחה_בן-עמרה', 'title_en': 'BEN AMARA', 'collection': 'familyNames',
                             "keys": {'main_image_url', 'version', 'title_he_lc', 'parsed_doc',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_related_musictext',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_related_recieve_unit',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_related_place', 'item_id',
                                      'template_id', 'item_url', 'template_changeset_id', 'slug_en',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_source', 'changeset',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_multimedia_music',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_multimedia_movies',
                                      'source_id',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_multimedia_photos',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_related_personality',
                                      'title_he', 'title_he_suggest', 'main_thumbnail_image_url',
                                      'content_html_en', 'document_id',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_family_name',
                                      'title_en_lc',
                                      'related_documents__c6_beit_hatfutsot_bh_base_template_related_exhibition',
                                      'metadata',
                                      'title_en_suggest', 'source', 'slugs', 'content_html_he'}})

def test_sync_with_invalid_collection():
    es = given_empty_elasticsearch_instance()
    input_doc = get_clearmash_convert_resource_data()[0]
    assert input_doc["id"] == "115306"
    # the source_doc is synced to clearmash as well, so need to ensure collection is deleted from both
    del input_doc["collection"]
    del input_doc["source_doc"]["collection"]
    docs = assert_sync_processor([input_doc])
    assert_dict(docs[0], {'id': '115306', 'sync_msg': 'added to ES'})
    assert_dict(es_doc(es, "clearmash", '115306'), {"collection": "unknown"})

def test_sync_update():
    # do initial sync for a specific doc to ES
    es = given_empty_elasticsearch_instance()
    docs = assert_sync_processor([get_clearmash_convert_resource_data()[0]])
    assert_dict(docs[0], {'id': '115306',
                          'version': '6468918-f91ea044052746a2903d6ee60d9b374b',
                          'sync_msg': 'added to ES'})
    assert_dict(es_doc(es, "clearmash", '115306'), {'version': '6468918-f91ea044052746a2903d6ee60d9b374b',
                                                    'title_en': 'BEN AMARA',
                                                    'title_en_suggest': 'BEN AMARA',
                                                    'title_en_lc': 'ben amara',
                                                    'slug_en': 'familyname_ben-amara',
                                                    'slugs': ['familyname_ben-amara', 'שםמשפחה_בן-עמרה']})
    # update the item title in the input doc, but don't change the version
    input_doc = get_clearmash_convert_resource_data()[0]
    input_doc["title_en"] = "updated title"
    docs = assert_sync_processor([input_doc])
    assert_dict(docs[0], {'id': '115306',
                          'version': '6468918-f91ea044052746a2903d6ee60d9b374b',
                          'sync_msg': 'no update needed'})
    # title was not updated in ES because changed are synced based on version
    assert_dict(es_doc(es, "clearmash", "115306"), {"title_en": "BEN AMARA"})
    # change the version as well
    input_doc = get_clearmash_convert_resource_data()[0]
    input_doc.update(title_en="updated title", version="updated version")
    docs = assert_sync_processor([input_doc])
    assert_dict(docs[0], {'id': '115306',
                          'version': 'updated version',
                          'sync_msg': 'updated doc in ES (old version = "6468918-f91ea044052746a2903d6ee60d9b374b")'})
    # title was updated
    assert_dict(es_doc(es, "clearmash", "115306"), {"title_en": "updated title"})

def test_slugs():
    es = given_empty_elasticsearch_instance()
    input_doc = get_clearmash_convert_resource_data()[0]
    # slugs are not present in the data from clearmash
    assert "slug_he" not in input_doc
    assert "slug_he" not in input_doc["source_doc"]
    assert_dict(input_doc, {'title_he': 'בן עמרה', 'title_en': 'BEN AMARA'})
    doc = assert_sync_processor([input_doc])[0]
    assert_dict(doc, {'id': '115306', 'sync_msg': 'added to ES'})
    # slugs are generated from the titles by the sync processor
    assert_dict(es_doc(es, "clearmash", "115306"), {'slug_he': 'שםמשפחה_בן-עמרה', 'slug_en': 'familyname_ben-amara'})
    input_doc = get_clearmash_convert_resource_data()[0]
    # update the same doc - delete the titles
    del input_doc["title_en"]
    del input_doc["title_he"]
    input_doc["version"] = "updated version"
    doc = assert_sync_processor([input_doc])[0]
    assert_dict(doc, {'id': '115306', 'sync_msg': 'updated doc in ES (old version = "6468918-f91ea044052746a2903d6ee60d9b374b")'})
    # slugs are never deleted, instead the new slug was added (in this case a default slug because there was no title)
    assert_dict(es_doc(es, "clearmash", "115306"), {'slug_he': 'שםמשפחה_בן-עמרה',
                                                    'slug_en': ['familyname_115306', 'familyname_ben-amara']})

def test_item_without_content_should_not_be_synced():
    assert_item_missing_content(content_html_en="del", content_html_he="del", is_synced=False)
    assert_item_missing_content(content_html_en="a", content_html_he="del", is_synced=True)
    assert_item_missing_content(content_html_en="", content_html_he="", is_synced=False)
    assert_item_missing_content(content_html_en="a", content_html_he="", is_synced=True)
    assert_item_missing_content(content_html_en=None, content_html_he=None, is_synced=False)
    assert_item_missing_content(content_html_en="a", content_html_he=None, is_synced=True)

def test_unique_slugs():
    es = given_empty_elasticsearch_instance()
    input_doc = get_clearmash_convert_resource_data()[0]
    assert_sync_processor([input_doc])
    # got the standard slugs
    assert_dict(es_doc(es, "clearmash", "115306"), {'slugs': ['familyname_ben-amara', 'שםמשפחה_בן-עמרה']})
    # add a different doc in the same collection which will have the same slug (case insensitive)
    input_doc = get_clearmash_convert_resource_data()[1]
    assert input_doc["id"] == "115325"
    input_doc.update(title_en="Ben AmArA", version="2", collection="familyNames")
    assert_sync_processor([input_doc])
    # got a different slug, to prevent slug conflict
    assert_dict(es_doc(es, "clearmash", "115325"), {'slugs': ['familyname_ben-amara-115325', 'שםמשפחה_נשאטל']})
