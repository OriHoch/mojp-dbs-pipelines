# mojp-dbs-pipelines

[![Travis](https://img.shields.io/travis/beit-Hatfutsot/mojp-dbs-pipelines/master.svg)](https://travis-ci.org/beit-Hatfutsot/mojp-dbs-pipelines)

Pipelines for data sync of Jewish data sources to the DB of the muesum of the Jewish people

Uses the [datapackage pipelines framework](https://github.com/frictionlessdata/datapackage-pipelines)

## Overview

This project provides pipelines that sync data from multiple external sources to the [MoJP](http://dbs.bh.org.il/) Elasticsearch DB.

## Usage
```
$ pip install mojp-dbs-pipelines
$ export CLEARMASH_API_KEY=""
$ export MOJP_DBS_ELASTICSEARCH_HOST="localhost:9200"
$ export MOJP_DBS_ELASTICSEARCH_INDEX="mojp"
$ mojp-dbs-dpp run clearmash
```
Now you will have all Clearamsh items in the Elasticsearch DB indexed according to MoJP requirements

## Development

Check out the [contribution guide](CONTRIBUTING.md)

## Data Sources

### MoJP Clearmash

Clearmash is A CMS system which is used by MoJP for the MoJP own data

Clearmash exposes an [API](https://bh.clearmash.com/API/V5/Services/) to get the data
