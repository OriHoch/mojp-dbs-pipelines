from mojp_dbs_pipelines.clearmash.download import TABLE_SCHEMA, download
from mojp_dbs_pipelines.common.processors.base_processors import AddResourcesProcessor


class AddClearmashResources(AddResourcesProcessor):

    def _get_resource_descriptors(self):
        return [{"name": "clearmash",
                 "path": "clearmash.csv",
                 "schema": TABLE_SCHEMA}]

    def _get_resources_iterator(self):
        return [download()]


if __name__ == '__main__':
    AddClearmashResources.main()
