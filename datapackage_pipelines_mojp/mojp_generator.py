import json
import os

from datapackage_pipelines.generators.generator_base import GeneratorBase
from datapackage_pipelines.generators.utilities import steps
from datapackage_pipelines_mojp.clearmash.constants import CONTENT_FOLDERS as CLEARMASH_CONTENT_FOLDERS

from .settings import MOJP_ONLY_DOWNLOAD, MOJP_MOCK


class MojpGenerator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        with open(os.path.join(os.path.dirname(__file__), "source-spec-schema.json")) as f:
            return json.load(f)

    @classmethod
    def generate_pipeline(cls, source):
        for dataSource in source["data-sources"]:
            if dataSource == "clearmash":
                # for clearmash we split it into multiple pipelines for each content folder
                for folder_id, folder in CLEARMASH_CONTENT_FOLDERS.items():
                    name = "{}_{}".format(dataSource, folder["collection"])
                    yield name, cls.get_pipeline_details(name, dataSource=dataSource, download_parameters={"folder_id": folder_id})
            else:
                yield dataSource, cls.get_pipeline_details(dataSource)

    @classmethod
    def get_pipeline_details(cls, name, dataSource=None, download_parameters=None, convert_parameters=None, sync_parameters=None):
        if dataSource is None:
            dataSource = name
        return {"title": name,
                "pipeline": steps(*cls.get_steps(name, dataSource, download_parameters, convert_parameters, sync_parameters))}

    @classmethod
    def get_steps(cls, name, dataSource, download_parameters=None, convert_parameters=None, sync_parameters=None):
        steps = [("add_metadata", cls.get_metadata(name))]
        for step, parameters in {"download": download_parameters,
                                 "convert": convert_parameters,
                                 "sync": sync_parameters}.items():
            if not cls.skip_step(name, dataSource, step, parameters):
                steps.append(cls.get_step(name, dataSource, step, parameters))
        steps.append(("dump.to_path", {"out-path": cls.get_outpath(name)}))
        return steps

    @classmethod
    def get_step(cls, name, dataSource, step, parameters=None):
        module = "datapackage_pipelines_mojp.{}.processors".format("common" if step == "sync" else dataSource)
        return ("{}.{}".format(module, step), cls.get_step_params(name, dataSource, step, parameters))

    @classmethod
    def get_step_params(cls, name, dataSource, step, override_parameters=None):
        parameters = {"mock": MOJP_MOCK}
        if override_parameters is not None:
            parameters.update(override_parameters)
        return parameters

    @classmethod
    def skip_step(cls, name, dataSource, step, parameters=None):
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
