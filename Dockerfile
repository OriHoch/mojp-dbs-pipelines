FROM frictionlessdata/datapackage-pipelines

RUN pip install --no-cache-dir pipenv pew
RUN apk --update --no-cache add build-base python3-dev bash jq libxml2 libxml2-dev git libxslt libxslt-dev

COPY Pipfile /pipelines/
COPY Pipfile.lock /pipelines/
RUN pipenv install --system --deploy --ignore-pipfile && pipenv check

COPY setup.py /pipelines/
RUN pip install -e .

COPY bagnowka /pipelines/bagnowka
COPY bin /pipelines/bin
COPY clearmash /pipelines/clearmash
COPY datapackage_pipelines_mojp /pipelines/datapackage_pipelines_mojp
