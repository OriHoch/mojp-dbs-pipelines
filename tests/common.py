from datapackage_pipelines.utilities.lib_test_helpers import mock_processor_test
import os

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


def listify_resources(resources):
    return [[row for row in resource] for resource in resources]


def assert_processor(processor_class, parameters, datapackage, resources, expected_datapackage, expected_resources):
    datapackage, resources = processor_class(parameters, datapackage, resources).spew()
    assert datapackage == expected_datapackage
    assert listify_resources(resources) == expected_resources
