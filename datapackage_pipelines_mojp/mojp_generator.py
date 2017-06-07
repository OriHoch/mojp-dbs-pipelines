import json
import os

from datapackage_pipelines.generators.generator_base import GeneratorBase
from datapackage_pipelines.generators.utilities import steps

from .settings import MOJP_ONLY_DOWNLOAD, MOJP_MOCK


class MojpGenerator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        with open(os.path.join(os.path.dirname(__file__), "source-spec-schema.json")) as f:
            return json.load(f)

    @classmethod
    def generate_pipeline(cls, source):
        for dataSource in source["data-sources"]:
            yield dataSource, cls.get_pipeline_details(dataSource)

    @classmethod
    def get_pipeline_details(cls, name):
        return {"title": "Download, Convert and Sync documents from {} to MoJP Databases".format(name),
                "pipeline": steps(*cls.get_steps(name))}

    @classmethod
    def get_steps(cls, name):
        steps = [("add_metadata", cls.get_metadata(name))]
        for step in ["download", "convert", "sync"]:
            if not cls.skip_step(name, step):
                steps.append(cls.get_step(name, step))
        steps.append(("dump.to_path", {"out-path": cls.get_outpath(name)}))
        return steps

    @classmethod
    def get_step(cls, name, step):
        module = "datapackage_pipelines_mojp.{}.processors".format("common" if step == "sync" else name)
        return ("{}.{}".format(module, step), cls.get_step_params(name, step))

    @classmethod
    def get_step_params(cls, name, step):
        return {
            "mock": MOJP_MOCK
        }

    @classmethod
    def skip_step(cls, name, step):
        if MOJP_ONLY_DOWNLOAD:
            return step != "download"
        else:
            return False

    @classmethod
    def get_metadata(cls, name):
        return {"name": cls.get_full_name(name)}

    @classmethod
    def get_outpath(cls, name):
        return "data/{}".format(cls.get_full_name(name))

    @classmethod
    def get_full_name(cls, name):
        if MOJP_MOCK:
            name = "mock-{}".format(name)
        if MOJP_ONLY_DOWNLOAD:
            name = "{}-download".format(name)
        return name
