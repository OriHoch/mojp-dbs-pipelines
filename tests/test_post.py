from datapackage_pipelines_mojp.common.processors.post import Processor
from .clearmash.test_convert import get_clearmash_convert_resource_data
from .test_sync import assert_sync_processor
from .common import (given_empty_elasticsearch_instance, get_mock_settings, assert_dict, es_doc,
                     assert_processor)

def assert_post_processor(input_data, parameters):
    processor = Processor(parameters=dict({"resource": "dbs-docs-sync-log"}, **parameters),
                          datapackage={"resources": [{"name": "dbs-docs-sync-log"}]},
                          resources=[input_data],
                          settings=get_mock_settings())
    return assert_processor(processor)

def test_delete():
    es = given_empty_elasticsearch_instance()
    dbs_docs = get_clearmash_convert_resource_data()
    dbs_docs = [dbs_docs[0], dbs_docs[1]]
    assert_dict(dbs_docs[0], {"source": "clearmash", "collection": "familyNames", "id": "115306"})
    assert_dict(dbs_docs[1], {"source": "clearmash", "collection": "places", "id": "115325"})
    dbs_docs.append(dict(dbs_docs[0], source="foobar", id="115308"))
    dbs_docs.append(dict(dbs_docs[1], source="foobar", id="115309"))
    sync_log = assert_sync_processor(dbs_docs)
    assert_post_processor(sync_log, {"all_items_query": {"source": "clearmash", "collection": "familyNames"}})
    # after sync and post, all items are in ES
    assert_dict(es_doc(es, "clearmash", "115306"), {"source": "clearmash", "collection": "familyNames"})
    assert_dict(es_doc(es, "clearmash", "115325"), {"source": "clearmash", "collection": "places"})
    assert_dict(es_doc(es, "foobar", "115308"), {"source": "foobar", "collection": "familyNames"})
    assert_dict(es_doc(es, "foobar", "115309"), {"source": "foobar", "collection": "places"})
    # run sync again, but this time without the clearmash family name
    dbs_docs = get_clearmash_convert_resource_data()
    sync_log = assert_sync_processor([dbs_docs[1]])
    # we are running over clearmash / familyNames - so only the family name will be deleted
    assert_post_processor(sync_log, {"all_items_query": {"source": "clearmash", "collection": "familyNames"}})
    # the family name was deleted
    assert not es_doc(es, "clearmash", "115306")
    # other items are not
    assert es_doc(es, "clearmash", "115325")
    assert es_doc(es, "foobar", "115308")
    assert es_doc(es, "foobar", "115309")
