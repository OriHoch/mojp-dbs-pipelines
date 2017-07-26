from datapackage_pipelines_mojp.clearmash.processors.download import Processor as DownloadProcessor


class Processor(DownloadProcessor):

    def __init__(self, *args, **kwargs):
        super(Processor, self).__init__(*args, **kwargs)
        self._all_downloaded_document_ids = []
        related_documents_config = self._parameters.get("related-documents")
        if related_documents_config:
            table = self.db_meta.tables.get(related_documents_config["table"])
            if table is not None:
                document_id_col = getattr(table.c, related_documents_config["document-id-column"])
                self._all_downloaded_document_ids = [row[0] for row
                                                     in self.db_session.query(document_id_col).all()]

    def _fetch_related_documents(self, related_documents):
        for cm_row in related_documents.get_related_documents():
            document_id = cm_row["document_id"]
            if document_id not in self._all_downloaded_document_ids:
                self._all_downloaded_document_ids.append(document_id)
                cm_row.update(collection="")
                yield cm_row

    def _filter_resource(self, resource_descriptor, resource_data):
        for row in resource_data:
            if self._check_override_item(row):
                for field_id, related_documents in self._get_clearmash_api().related_documents.get_for_doc(row).items():
                    related_document_ids = related_documents.first_page_results
                    if len(related_document_ids) > 0:
                        if len(self._all_downloaded_document_ids) == 0:
                            num_exists = 0
                        else:
                            num_exists = sum([1 for doc_id in self._all_downloaded_document_ids if doc_id in related_document_ids])
                        if self._parameters.get("only-download-new"):
                            if num_exists == 0:
                                yield from self._fetch_related_documents(related_documents)
                        elif num_exists < len(related_document_ids):
                            yield from self._fetch_related_documents(related_documents)


if __name__ == '__main__':
    Processor.main()
