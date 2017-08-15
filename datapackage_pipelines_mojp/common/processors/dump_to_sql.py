from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from jsontableschema_sql.mappers import descriptor_to_columns_and_constraints
from sqlalchemy import Table
import logging


class Processor(BaseProcessor):

    @property
    def _db_table(self):
        return self.db_meta.tables.get(self._tablename)

    def _create_table(self):
        if self._db_table is None:
            columns, constraints, indexes = descriptor_to_columns_and_constraints("", self._tablename, self._schema, (), None)
            Table(self._tablename, self.db_meta, *(columns + constraints + indexes)).create()
            self._stats["was table created?"] = "yes"
            logging.info("created table {}".format(self._tablename))

    def _filter_row(self, row):
        self._row_num += 1
        if self._row_num%self._commit_buffer_length == 0:
            logging.info("commit ({} updated, {} inserted)".format(self._stats["number of updated rows"], self._stats["number of inserted rows"]))
            self.db_commit()
        filter_args = (getattr(self._db_table.c, field)==row[field] for field in self._update_keys)
        table = self._db_table
        if self.db_session.query(table).filter(*filter_args).count() > 0:
            # update
            values = {field: row[field] for field in row if field not in self._update_keys}
            stmt = table.update()
            for arg in filter_args:
                stmt.where(arg)
            stmt.values(**values).execute()
            self._stats["number of updated rows"] += 1
        else:
            # insert
            table.insert().values(**row).execute()
            self._stats["number of inserted rows"] += 1
        return row

    def _filter_resource(self, *args):
        self._stats["number of updated rows"] = 0
        self._stats["number of inserted rows"] = 0
        self._stats["was table created?"] = "no"
        self._tablename = self._parameters["table"]
        self._schema = self._get_resource_descriptor()["schema"]
        self._update_keys = self._schema["primaryKey"]
        self._create_table()
        self._commit_buffer_length = int(self._parameters.get("commit-every", "100"))
        self._stats["committed every X rows"] = self._commit_buffer_length
        self._row_num = 0
        logging.info("committing every {} rows".format(self._stats["committed every X rows"]))
        yield from super(Processor, self)._filter_resource(*args)
        self.db_commit()


if __name__ == '__main__':
    Processor.main()
