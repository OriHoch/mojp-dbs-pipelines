from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
from jsontableschema_sql.mappers import descriptor_to_columns_and_constraints
from sqlalchemy import Table
import logging
from sqlalchemy.ext.automap import automap_base


class Processor(BaseProcessor):

    def _create_table(self):
        columns, constraints, indexes = self._descriptor_to_columns_and_constraints("", self._tablename, self._schema,
                                                                                    (), None)
        table = Table(self._tablename, self.db_meta, *(columns + constraints + indexes))
        table.create()
        self._stats["was table created?"] = "yes"
        logging.info("{}: created table".format(self._tablename))
        return table

    def _descriptor_to_columns_and_constraints(self, *args):
        return descriptor_to_columns_and_constraints(*args)

    def _get_mapper(self):
        Base = automap_base(metadata=self.db_meta)
        Base.prepare()
        return getattr(Base.classes, self._tablename)

    def db_commit(self):
        if len(self._rows_buffer) > 0:
            self.db_connect(retry=True)
            table = self.db_meta.tables.get(self._tablename)
            if table is None:
                table = self._create_table()
            mapper = self._get_mapper()
            update_rows = []
            insert_rows = []
            for row in self._rows_buffer:
                filter_args = (getattr(table.c, field)==row[field] for field in self._update_keys)
                if self.db_session.query(table).filter(*filter_args).count() > 0:
                    update_rows.append(row)
                else:
                    insert_rows.append(row)
            if len(insert_rows) > 0:
                self.db_session.bulk_insert_mappings(mapper, insert_rows)
                self._stats["number of inserted rows"] += len(insert_rows)
            if len(update_rows) > 0:
                self.db_session.bulk_update_mappings(mapper, update_rows)
                self._stats["number of updated rows"] += len(update_rows)
            super(Processor, self).db_commit()
            logging.info("{}: commit ({} updated, {} inserted)".format(self._tablename,
                                                                       self._stats["number of updated rows"],
                                                                       self._stats["number of inserted rows"]))
            self._rows_buffer = []
            # force a new session on next commit
            self._db_delete_session()

    def _db_delete_session(self):
        delattr(self, "_db_session")

    def _filter_row(self, row):
        self._row_num += 1
        if self._commit_buffer_length > 1:
            if self._row_num%self._commit_buffer_length == 0:
                self.db_commit()
        self._rows_buffer.append(row)
        if self._commit_buffer_length < 2:
            self.db_commit()
        return row

    def _filter_resource_init(self, schema):
        self._stats["number of updated rows"] = 0
        self._stats["number of inserted rows"] = 0
        self._stats["was table created?"] = "no"
        self._tablename = self._parameters["table"]
        self._schema = schema
        self._update_keys = self._schema["primaryKey"]
        self._commit_buffer_length = int(self._parameters.get("commit-every", "100"))
        self._stats["committed every X rows"] = self._commit_buffer_length
        self._row_num = 0
        self._rows_buffer = []
        self.db_connect()
        if self._commit_buffer_length > 1:
            logging.info("{}: initialized, committing every {} rows".format(self._tablename,
                                                                            self._stats["committed every X rows"]))
        else:
            logging.info("{}: initialized".format(self._tablename))

    def _filter_resource(self, *args):
        self._filter_resource_init(self._get_resource_descriptor()["schema"])
        yield from super(Processor, self)._filter_resource(*args)
        self.db_commit()

    @classmethod
    def initialize(cls, table_name, schema, settings=None):
        processor = cls({"resource": "_", "table": table_name, "commit-every": 0},
                        {"name": "_", "resources": [{"name": "_", "schema": schema}]},
                        [], settings)
        processor._filter_resource_init(schema)
        return processor

    def commit_rows(self, rows):
        self._rows_buffer = rows
        self.db_commit()

    @property
    def stats(self):
        return self._stats

if __name__ == '__main__':
    Processor.main()
