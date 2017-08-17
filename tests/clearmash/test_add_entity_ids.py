from datapackage_pipelines_mojp.clearmash.processors.add_entity_ids import Processor as AddEntityIdsProcessor
from tests.clearmash.mock_clearmash_api import MockClearmashApi
from tests.common import get_mock_settings, assert_conforms_to_schema
from datapackage_pipelines_mojp.common.processors.dump_to_sql import Processor as DumpToSqlProcessor
from tests.common import get_test_db_session
from sqlalchemy import Column, Text
import json


class MockDumpToSqlProcessor(DumpToSqlProcessor):

    def __init__(self, *args, **kwargs):
        self._jsonb_columns = []
        super(MockDumpToSqlProcessor, self).__init__(*args, **kwargs)
    
    def _get_new_db_session(self):
        if not hasattr(self, "__test_db_session"):
            self.__test_db_sssion = get_test_db_session()
        return self.__test_db_sssion

    def _descriptor_to_columns_and_constraints(self, *args):
        columns, constraints, indexes = super(MockDumpToSqlProcessor, self)._descriptor_to_columns_and_constraints(*args)
        columns = [self._filter_sqlalchemy_column(column) for column in columns]
        return columns, constraints, indexes

    def _filter_sqlalchemy_column(self, column):
        # change JSONB to TEXT because sqlite doesn't support jsonb
        if str(column.type) == "JSONB":
            self._jsonb_columns.append(column.name)
            column = Column(column.name, Text())
        return column

    def _filter_sqlalchemy_column_value(self, k, v):
        if k in self._jsonb_columns:
            return json.dumps(v)
        else:
            return v

    def _filter_sqlalchemy_row(self, row):
        return {k: self._filter_sqlalchemy_column_value(k, v) for k, v in row.items()}

    def db_commit(self):
        return [self._filter_sqlalchemy_row(row) for row in self._rows_buffer]

    def db_connect(self, **kwargs):
        pass

    def _db_delete_session(self):
        pass


class MockAddEntityIdsProcessor(AddEntityIdsProcessor):

    def _get_clearmash_api_class(self):
        return MockClearmashApi

    def _get_new_db_session(self):
        if not hasattr(self, "__test_db_session"):
            self.__test_db_sssion = get_test_db_session()
        return self.__test_db_sssion

    def _get_folders_processor(self, parameters, schema, settings):
        processor = MockDumpToSqlProcessor({"resource": "_",
                                            "table": "clearmash-folders",
                                            "commit-every": 0},
                                           {"name": "_", "resources": [{"name": "_", "schema": schema}]},
                                           [], settings)
        processor.__test_db_sssion = self._get_new_db_session()
        processor._filter_resource_init(schema)
        return processor


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
