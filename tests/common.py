import os

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


def listify_resources(resources):
    return [[row for row in resource] for resource in resources]


def assert_processor(processor_class, mock_settings=None, parameters=None, datapackage=None, resources=None,
                     expected_datapackage=None, expected_resources=None):
    if not mock_settings:
        mock_settings = type("MockSettings", (object,), {})
    if not parameters:
        parameters = {}
    if not datapackage:
        datapackage = {}
    if not resources:
        resources = []
    if not expected_datapackage:
        expected_datapackage = {}
    if not expected_resources:
        expected_resources = []
    datapackage, resources = processor_class(parameters, datapackage, resources, mock_settings).spew()
    assert datapackage == expected_datapackage, "expected={}, actual={}".format(expected_datapackage, datapackage)
    if expected_resources:
        assert listify_resources(resources) == expected_resources, \
            "expected={}, actual={}".format(expected_resources, actual_resources)
    else:
        return resources
