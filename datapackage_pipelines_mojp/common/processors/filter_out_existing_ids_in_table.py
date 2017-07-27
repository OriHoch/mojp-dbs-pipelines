from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging


class Processor(BaseProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._db_table = self.db_meta.tables.get(self._parameters["table"])
        if self._db_table is not None:
            self._id_column = getattr(self._db_table.c, self._parameters["id-column"])
            self._existing_ids = self._get_existing_ids()
        else:
            self._existing_ids = []

    def _get_existing_ids(self):
        return [int(row[0]) for row in self.db_session.query(self._id_column).all()]

    def _filter_row(self, row):
        if int(row[self._parameters["id-field"]]) in self._existing_ids:
            return None
        else:
            # logging.info("new id: {}".format(row[self._parameters["id-field"]]))
            return row

if __name__ == '__main__':
    Processor.main()
