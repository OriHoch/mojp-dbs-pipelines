from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging, datetime, importlib
from datapackage_pipelines_mojp.common.utils import parse_import_func_parameter


class Processor(BaseProcessor):

    def _filter_update_value(self, v, curval):
        return parse_import_func_parameter(v, curval)

    def _filter_row(self, row):
        table = self.db_meta.tables[self._parameters["table"]]
        for k, v in self._parameters["fields"].items():
            row[k] = self._filter_update_value(v, row.get(k))
        table.update()\
            .where(getattr(table.c, self._parameters["id-column"])==row[self._parameters["id-field"]])\
            .values(**row)\
            .execute()
        self.db_commit()
        return row

if __name__ == '__main__':
    Processor.main()
