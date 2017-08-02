from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging
from datapackage_pipelines_mojp.common.utils import parse_import_func_parameter


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)

    def _process(self, datapackage, resources):
        self._db_table = self.db_meta.tables.get(self._parameters["table"])
        if self._db_table is not None:
            self._id_column = getattr(self._db_table.c, self._parameters["id-column"])
            self._existing_ids = [int(row[0]) for row in self.db_session.query(self._id_column).all()]
        else:
            self._existing_ids = []
        return super(Processor, self)._process(datapackage, resources)

    def _filter_row(self, row):
        is_in_existing_ids = int(row[self._parameters["id-field"]]) in self._existing_ids
        filter_row = self._parameters.get("filter-row")
        if filter_row:
            return parse_import_func_parameter(filter_row, row, is_in_existing_ids)
        elif is_in_existing_ids:
            return None
        else:
            return row

if __name__ == '__main__':
    Processor.main()
