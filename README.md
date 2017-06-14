# Datapackage pipelines for The Museum of The Jewish People

[![Travis](https://img.shields.io/travis/Beit-Hatfutsot/mojp-dbs-pipelines/master.svg)](https://travis-ci.org/Beit-Hatfutsot/mojp-dbs-pipelines)

Pipelines for data sync of Jewish data sources to the DB of The Museum of The Jewish People

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

Docker is used to get the full environment that runs the sync pipelines. see [README-DOCKER](README-DOCKER.md) for more details, tips and common problems with Docker

* [Install Docker](https://docs.docker.com/engine/installation/) (tested with version 17.03)
* [Install Docker Compose](https://docs.docker.com/compose/install/)

Once you have docker you can run the following to build and start the app:

* `make docker-clean-start`

see [README-DOCKER](README-DOCKER.md) for more details

## Usage

* go to: http://localhost:5000/
* the datapackage-pipelines framework runs the pipelines automatically according to the pipelines configurations

## Development

Check out the [contribution guide](CONTRIBUTING.md)

## Available Data Sources

### Clearmash

Clearmash is A CMS system which is used by MoJP for the MoJP own data

Clearmash exposes an [API](https://bh.clearmash.com/API/V5/Services/) to get the data

relevant links and documentation (clearmash support site requires login)
* [overview of the services and api urls](https://www.clearmash.com/skn/c7/Support/e1043/External_Resources_API__Server_API_)
