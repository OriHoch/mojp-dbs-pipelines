FROM python:3.6-alpine

# install system requirements
RUN apk add --update --no-cache --virtual=build-dependencies \
    antiword \
    build-base \
    curl \
    jpeg-dev \
    libxml2-dev libxml2 \
    libxslt-dev libxslt \
    libstdc++ \
    libpq \
    python3-dev postgresql-dev
RUN apk --repository http://dl-3.alpinelinux.org/alpine/edge/testing/ --update add leveldb leveldb-dev
RUN pip install psycopg2 datapackage-pipelines-github lxml datapackage-pipelines[speedup]

ENV PYTHONUNBUFFERED 1

RUN mkdir /mojp
WORKDIR /mojp
ADD . /mojp/

# install the mojp-dbs-pipelines package
RUN pip install -e /mojp

ENTRYPOINT ["/mojp/docker-run.sh"]
