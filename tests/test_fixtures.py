from datapackage_pipelines.utilities.lib_test_helpers import ProcessorFixtureTestsBase, rejsonize
import os
from jinja2 import Template
import json

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


class MojpDbsPipelinesFixtureTests(ProcessorFixtureTestsBase):

    def _get_processor_file(self, processor):
        return os.path.join(ROOT_PATH, "mojp_dbs_pipelines", processor.strip())

    def get_tests(self):
        for filename, func in super(MojpDbsPipelinesFixtureTests, self).get_tests():
            yield filename.replace(".jinja2", ""), func

    def _load_fixture(self, dirpath, filename):
        with open(os.path.join(dirpath, filename), encoding='utf8') as f:
            template_file = f.read()
        if filename.endswith(".jinja2"):
            rendered_file = Template(template_file).render(self.get_context(filename))
        else:
            rendered_file = template_file
        parts = rendered_file.split('--\n')
        processor, params, dp_in, data_in, dp_out, data_out = parts
        processor_file = self._get_processor_file(processor)
        params = rejsonize(params)
        dp_out = rejsonize(dp_out)
        dp_in = rejsonize(dp_in)
        data_in = (dp_in + '\n\n' + data_in).encode('utf8')
        print(data_out.split("\n"))
        return data_in, data_out, dp_out, params, processor_file

    def get_context(self, filename):
        context = {}
        if filename == "clearmash_add_resources.jinja2":
            mock_downloaded_rows = [json.dumps({"id": 1, "doc": json.dumps({"title": "foobar", "content": "bazbax"})}),
                                    json.dumps({"id": 2, "doc": json.dumps({"title": "222", "content": "2222"})})]
            context["MOCK_DOWNLOADED_ROWS"] = "\n".join(mock_downloaded_rows)
        return context


for filename, func in MojpDbsPipelinesFixtureTests(os.path.join(os.path.dirname(__file__), 'fixtures')).get_tests():
    globals()['test_fixture_%s' % filename] = func
