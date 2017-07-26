from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from datapackage import DataPackage
import os, csv, sys

csv.field_size_limit(sys.maxsize)


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

    def _get_load_resources(self):
        if not hasattr(self, "_load_resources"):
            self._load_resources = []
            for load_resource in self._parameters["load-resources"]:
                if os.path.exists(load_resource["url"]):
                    datapackage = DataPackage(load_resource["url"])
                    for resource in datapackage.resources:
                        if resource.descriptor["name"] == load_resource["resource"]:
                            self._load_resources.append(resource)
        return self._load_resources

    def _get_schema(self):
        load_resources = self._get_load_resources()
        if len(load_resources) == 0:
            return None
        else:
            return load_resources[0].descriptor["schema"]

    def _get_resource(self):
        for resource in self._get_load_resources():
            yield from resource.iter()


if __name__ == '__main__':
    Processor.main()
