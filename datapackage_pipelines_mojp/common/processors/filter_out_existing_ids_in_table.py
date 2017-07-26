from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        db_table = self.db_meta.tables.get(self._parameters["table"])
        if db_table is not None:
            self._existing_ids = [int(row[getattr(db_table.c, self._parameters["id-column"])])
                                  for row in db_table.select().execute().fetchall()]
        else:
            self._existing_ids = []

    def _filter_row(self, row):
        if int(row[self._parameters["id-field"]]) in self._existing_ids:
            return None
        else:
            logging.info("new id: {}".format(row[self._parameters["id-field"]]))
            return row

if __name__ == '__main__':
    Processor.main()
