from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase
import os

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


class MojpDbsPipelinesFixtureTests(ProcessorFixtureTestsBase):

    def _get_processor_file(self, processor):
        return os.path.join(ROOT_PATH, "mojp_dbs_pipelines", processor.strip())

    def _replace_tokens(self, instr):
        objs = [{"id": 1, "doc": "{\"title\": \"foobar\", \"content\": \"bazbax\"}"},
                {"id": 2, "doc": "{\"title\": \"222\", \"content\": \"2222\"}"}]
        objs_str = [str(obj) for obj in objs]
        MOCK_DOWNLOADED_ROWS = "\n".join(objs_str)
        instr = instr.decode() if hasattr(instr, "decode") else instr
        outstr = instr.replace("--MOCK_DOWNLOADED_ROWS--",
                               MOCK_DOWNLOADED_ROWS)
        return outstr.encode("utf-8")

    def _load_fixture(self, dirpath, filename):
        data_in, data_out, dp_out, params, processor_file = super(MojpDbsPipelinesFixtureTests, self)._load_fixture(dirpath, filename)
        return self._replace_tokens(data_in), self._replace_tokens(data_out), self._replace_tokens(dp_out), params, processor_file


for filename, testfunc in MojpDbsPipelinesFixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_fixture_%s' % filename] = testfunc
