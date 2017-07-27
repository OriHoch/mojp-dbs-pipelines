# Datapackage pipelines for The Museum of The Jewish People

[![Travis](https://img.shields.io/travis/Beit-Hatfutsot/mojp-dbs-pipelines/master.svg)](https://travis-ci.org/Beit-Hatfutsot/mojp-dbs-pipelines)

Pipelines for data sync of Jewish data sources to the DB of The Museum of The Jewish People

Uses the [datapackage pipelines framework](https://github.com/frictionlessdata/datapackage-pipelines)


## Overview

This project provides pipelines that sync data from multiple external sources to the [MoJP](http://dbs.bh.org.il/) Elasticsearch DB.


### Running the full pipelines environment using docker

* Install Docker and Docker Compose (refer to Docker guides for your OS)
* `cp docker-compose.override.yml.example.full docker-compose.override.yml`
* edit docker-compose.override.yml and modify settings (most likely you will need to set the CLEARMASH_CLIENT_TOKEN
* `make docker-pull`
* `make docker-build-all`
* `make docker-start`

This will provide:

* Pipelines dashboard: http://localhost:5000/
* PostgreSQL server: postgresql://postgres:123456@localhost:15432/postgres
* Elasticsearch server: localhost:19200
* Data files under: .data-docker/

After every change in the code you should run `make docker-build && make docker-start`


### Running the pipelines locally

Make sure you have Python 3.6 in a virtualenv

* `make install`
* `make test`
* `dpp`


### Connecting the local pipelines to the docker services

Assuming you created your docker-compose based on docker-compose.override.yml.example.full

* edit docker-compose.override.yml and uncomment the line that disable the app (not required but recommended)
* copy the elasticsearch and postgresql variables from .env.example.full to your .env file
* pip install psycopg2

Now, each time, before running you should setup the following environment variables:

* `source .env`
* `export DPP_DB_ENGINE=$DPP_DB_ENGINE`

Then you can run dpp

* `dpp`


## Available Data Sources

### Clearmash

Clearmash is A CMS system which is used by MoJP for the MoJP own data

Clearmash exposes an [API](https://bh.clearmash.com/API/V5/Services/) to get the data

relevant links and documentation (clearmash support site requires login)

* [overview of the services and api urls](https://www.clearmash.com/skn/c7/Support/e1043/External_Resources_API__Server_API_)
