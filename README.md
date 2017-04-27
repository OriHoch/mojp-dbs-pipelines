# mojp-dbs-pipelines

[![Travis](https://img.shields.io/travis/Beit-Hatfutsot/mojp-dbs-pipelines/master.svg)](https://travis-ci.org/Beit-Hatfutsot/mojp-dbs-pipelines)

Pipelines for data sync of Jewish data sources to the DB of the muesum of the Jewish people

Uses the [datapackage pipelines framework](https://github.com/frictionlessdata/datapackage-pipelines)

## Overview

This project provides pipelines that sync data from multiple external sources to the [MoJP](http://dbs.bh.org.il/) Elasticsearch DB.

## Downloading
```
$ wget https://github.com/Beit-Hatfutsot/mojp-dbs-pipelines/archive/master.zip -O mojp-dbs-pipelines.zip
$ unzip mojp-dbs-pipelines.zip
$ cd mojp-dbs-pipelines
```

## Installation
You should be inside a virtualenv with a supported Python version (3.6.1), see the [contribution guide](CONTRIBUTING.md) for more details
```
(mojp-dbs-pipelines) mojp-dbs-pipelines$ make install && make test
```

## Usage
Also, inside the activated python virtualenv
```
(mojp-dbs-pipelines) mojp-dbs-pipelines$ dpp
```

## Development

Check out the [contribution guide](CONTRIBUTING.md)

## Additional documentation

* [clearmash pipeline](/mojp_dbs_pipelines/clearmash/README.md)
