from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import json, logging
from sqlalchemy import *


class LoadSqlResource(BaseProcessor):

    def __init__(self, parameters=None, datapackage=None, resources=None):
        super(LoadSqlResource, self).__init__(parameters, datapackage, resources)
        self._db_table = self.db_meta.tables.get(self._parameters["load-table"])

    def _get_schema(self):
        with open(self._parameters["datapackage"]) as f:
            for resource in json.load(f)["resources"]:
                if resource["name"] == self._parameters["load-resource"]:
                    self._schema = resource["schema"]
                    return self._schema

    def _get_id_column(self):
        primary_keys = self._schema.get("primaryKey")
        if primary_keys and len(primary_keys) == 1:
            return getattr(self._db_table.c, primary_keys[0])
        else:
            raise Exception("failed to find an appropriate primary key in the schema")

    def _get_base_query(self):
        q = self.db_session.query(self._db_table).order_by(self._id_column)
        where = self._parameters.get("where")
        if where:
            q = q.filter(text(where))
        return q

    def _get_resource(self):
        if self._db_table is not None:
            self._id_column = self._get_id_column()
            num_total = self._get_base_query().count()
            batch_size = 10
            for i in range(0, num_total, batch_size):
                for db_row in self._get_base_query().slice(i, i+batch_size).all():
                    row = {}
                    for field in self._schema["fields"]:
                        val = getattr(db_row, field["name"])
                        row[field["name"]] = val
                    yield row
                logging.info("loaded {} / {}".format(i, num_total))


if __name__ == '__main__':
    LoadSqlResource.main()
