from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging, datetime
from copy import deepcopy


class Processor(BaseProcessor):

    def _filter_resource_descriptor(self, resource_descriptor):
        super(Processor, self)._filter_resource_descriptor(resource_descriptor)
        for field in self._parameters["fields"]:
            field = deepcopy(field)
            field.pop("value")
            resource_descriptor["schema"]["fields"].append(field)

    def _filter_row(self, row):
        for field in self._parameters["fields"]:
            value = field["value"]
            if field["type"] == "datetime" and value == "now":
                value = datetime.datetime.now()
            row[field["name"]] = value
        return row


if __name__ == '__main__':
    Processor.main()
