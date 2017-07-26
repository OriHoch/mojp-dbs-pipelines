from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import json, logging
from sqlalchemy import *


class LoadSqlResource(BaseProcessor):

    def __init__(self, parameters=None, datapackage=None, resources=None):
        super(LoadSqlResource, self).__init__(parameters, datapackage, resources)

    def _get_schema(self):
        with open(self._parameters["datapackage"]) as f:
            for resource in json.load(f)["resources"]:
                if resource["name"] == self._parameters["load-resource"]:
                    self._schema = resource["schema"]
                    return self._schema

    def _get_resource(self):
        table = self.db_meta.tables.get(self._parameters["load-table"])
        if table is not None:
            q = table.select()
            if self._parameters.get("where"):
                q = q.where(text(self._parameters["where"]))
            for db_row in q.execute().fetchall():
                row = {}
                for field in self._schema["fields"]:
                    val = db_row[getattr(table.c, field["name"])]
                    row[field["name"]] = val
                yield row


if __name__ == '__main__':
    LoadSqlResource.main()
