from datapackage_pipelines_mojp.clearmash.processors.download_related import Processor
from tests.clearmash.mock_clearmash_api import MockClearmashApi


class MockProcessor(Processor):

    def _get_clearmash_api(self):
        return MockClearmashApi()


if __name__ == "__main__":
    MockProcessor.main()
