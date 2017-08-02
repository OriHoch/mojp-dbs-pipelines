from datapackage_pipelines_mojp.common.processors.base_processors import BaseProcessor


class Processor(BaseProcessor):

    def _delete(self, id):
        raise NotImplementedError("TODO: delete id {} from elasticsearch".format(id))

    def _filter_row(self, row):
        id = row[self._parameters["id-field"]]
        source = self._parameters["source"]
        es_id = "{}_{}".format(id, source)
        display_allowed = row[self._parameters["display_allowed"]]
        if not display_allowed:
            self._delete(es_id)

if __name__ == '__main__':
    Processor.main()
