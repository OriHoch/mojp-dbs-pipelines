from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_mojp import settings as mojp_settings
from itertools import chain
from datapackage_pipelines_mojp.common.db import get_session, MetaData
import logging


class BaseProcessor(object):
    """
    all mojp processor should extend this class
    it is pluggable into our unit tests to allow mocks and automated tests of processors
    
    it runs in 3 main modes, depending on the parameters it gets from the pipeline:
    
    adding a new resource:
        parameters: add-resource = name of the resource to add
        relevant methods: _get_schema = return the schema for the new resource
                          _get_resource = yield the items for the new resource
    
    filtering an existing resource without changing schema:
        parameters: resource = name of the resource to filter
        relevant methods: _filter_row = filter each row of data
                          _filter_resource = filter over the entire resource (calls _filter_row)
    
    filtering a resource and modifying the schema
        parameters: input-resource = name of the input resource
                    output-resource = name of the output resource
        relevant methods: _get_schema = return the updated schema (optional, can be ommitted if output resource has the same schema)
                          _filter_row / _filter_resource = filter over the resource and yield according to the new schema
    """

    def __init__(self, parameters, datapackage, resources, settings=None):
        self._parameters = parameters
        self._datapackage = datapackage
        self._resources = resources
        self._settings = mojp_settings if not settings else settings
        self._add_resource = None
        self._input_resource = None
        self._output_resource = None
        if self._parameters.get("add-resource"):
            self._input_resource = self._output_resource = self._add_resource = self._parameters["add-resource"]
        elif self._parameters.get("resource"):
            self._input_resource = self._output_resource = self._parameters["resource"]
        else:
            self._input_resource = self._parameters["input-resource"]
            self._output_resource = self._parameters["output-resource"]
        self._stats = {}

    @classmethod
    def main(cls):
        # can be used like this in datapackage processor files:
        # if __main__ == '__main__':
        #      Processor.main()
        spew(*cls(*ingest()).spew())

    def spew(self):
        self._datapackage, self._resources = self._process(self._datapackage, self._resources)
        return self._datapackage, self._resources, self._get_stats()

    def _get_stats(self):
        return self._stats

    def _process(self, datapackage, resources):
        if self._add_resource:
            datapackage["resources"].append({"name": self._add_resource,
                                             "path": self._add_resource+".csv",
                                             "schema": self._get_schema()})
            return datapackage, chain(resources, [self._get_resource()])
        else:
            for resource_descriptor in datapackage["resources"]:
                if self._is_matching_input_resource(resource_descriptor):
                    self._filter_resource_descriptor(resource_descriptor)
            return datapackage, self._filter_resources(datapackage, resources)

    def _filter_resources(self, datapackage, resources):
        for resource_descriptor, resource_data in zip(datapackage["resources"], resources):
            if self._is_matching_output_resource(resource_descriptor):
                yield self._filter_resource(resource_descriptor, resource_data)

    def _filter_resource_descriptor(self, resource_descriptor):
        resource_descriptor.update(name=self._output_resource,
                                   path=self._output_resource+".csv")
        schema = self._get_schema()
        if schema:
            resource_descriptor.update(schema=schema)

    @classmethod
    def _get_schema(cls):
        return None

    def _is_matching_input_resource(self, resource_descriptor):
        return resource_descriptor["name"] == self._input_resource

    def _is_matching_output_resource(self, resource_descriptor):
        return resource_descriptor["name"] == self._output_resource

    def _filter_resource(self, resource_descriptor, resource_data):
        for row in resource_data:
            row = self._filter_row(row)
            if row:
                yield row

    def _filter_row(self, row):
        return row

    def _get_resource(self):
        pass

    def _get_settings(self, key=None, default=None):
        if key:
            if default is None and not hasattr(self._settings, key):
                raise Exception("unknown key: {}".format(key))
            else:
                return getattr(self._settings, key, default)
        else:
            return self._settings

    @property
    def db_session(self):
        if not hasattr(self, "_db_session"):
            self._db_session = self._get_new_db_session()
        return self._db_session

    @property
    def db_meta(self):
        if not hasattr(self, "_db_meta"):
            self._db_meta = MetaData(bind=self.db_session.connection())
            self._db_meta.reflect()
        return self._db_meta

    def db_commit(self):
        if hasattr(self, "_db_session"):
            self._db_session.commit()
            self._db_session = self._get_new_db_session()
            delattr(self, "_db_meta")

    def _get_new_db_session(self):
        return get_session()

    def _warn_once(self, msg):
        if not hasattr(self, "_warned_once"):
            self._warned_once = []
        if msg not in self._warned_once:
            self._warned_once.append(msg)
            logging.warning(msg)
