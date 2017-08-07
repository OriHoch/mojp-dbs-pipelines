from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor
import logging


class Processor(BaseProcessor):

    def _filter_resource(self, resource_descriptor, resource_data):
        self._db_table = self.db_meta.tables.get(self._parameters.get("table"))
        if self._db_table is None:
            raise Exception("db table does not exist, no point to delete items..")
        else:
            self._all_existing_ids = {int(row.item_id): row.display_allowed
                                      for row in self.db_session.query(self._db_table.c.item_id,
                                                                       self._db_table.c.display_allowed).all()}
        self._stats["all_existing_ids_length"] = len(self._all_existing_ids)
        self._stats["total_marked_for_deletion"] = 0
        yield from super(Processor, self)._filter_resource(resource_descriptor, resource_data)

    def _get_item_id(self, es_dbs_doc):
        return es_dbs_doc["item_id"]

    def _filter_row(self, es_dbs_doc):
        item_id = int(self._get_item_id(es_dbs_doc))
        display_allowed = self._all_existing_ids.get(item_id, False)
        if not display_allowed:
            # rows returned here will be deleted in ES
            self._stats["total_marked_for_deletion"] += 1
            logging.info(es_dbs_doc)
            return es_dbs_doc


if __name__ == '__main__':
    Processor.main()
