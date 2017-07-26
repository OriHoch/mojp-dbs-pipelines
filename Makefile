.PHONY: install install-optimized test docker-pull docker-build docker-build-all docker-start docker-clean docker-logs docker-stop docker-push deploy

DEPLOY_BRANCH ?= master

install:
	pip install -r requirements.txt
	pip install --upgrade -e .[develop]

install-optimized:
	pip install .

test:
	tox

docker-pull:
	docker pull orihoch/mojp-dbs-pipelines
	docker pull orihoch/mojp-elasticsearch
	docker pull orihoch/mojp-dbs-back

docker-build:
	docker build -t orihoch/mojp-dbs-pipelines .

docker-build-all:
	make docker-build
	docker build -t orihoch/mojp-elasticsearch dockers/elasticsearch
	docker build -t orihoch/mojp-kibana dockers/kibana
	docker build -t orihoch/mojp-dbs-back https://github.com/beit-hatfutsot/dbs-back.git

docker-start:
	mkdir -p .data-docker/elasticsearch
	mkdir -p .data-docker/postgresql
	docker-compose up -d

docker-logs:
	docker-compose logs

docker-clean:
	make docker-stop || true
	docker-compose rm -f

docker-stop:
	docker-compose stop

docker-push:
	docker push orihoch/mojp-dbs-pipelines

deploy:
	git pull origin $(DEPLOY_BRANCH)
	make docker-build-all
	make docker-start
