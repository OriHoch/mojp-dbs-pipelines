from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging, datetime


class Processor(BaseProcessor):

    def _filter_update_value(self, v):
        if v == "datetime-now":
            v = datetime.datetime.now()
        return v

    def _filter_row(self, row):
        table = self.db_meta.tables[self._parameters["table"]]
        table.update()\
            .where(getattr(table.c, self._parameters["id-column"])==row[self._parameters["id-field"]])\
            .values(**{k: self._filter_update_value(v) for k, v in self._parameters["fields"].items()})\
            .execute()
        self.db_commit()
        return row

if __name__ == '__main__':
    Processor.main()
