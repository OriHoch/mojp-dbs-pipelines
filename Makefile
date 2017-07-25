.PHONY: install install-optimized test docker-pull docker-build docker-start docker-clean docker-logs docker-logs-f docker-restart docker-clean-start docker-stop docker-push deploy

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

docker-start:
	mkdir -p .data-docker/elasticsearch
	mkdir -p .data-docker/postgresql
	docker-compose up -d

docker-logs:
	docker-compose logs

docker-logs-f:
	docker-compose logs -f

docker-restart:
	docker-compose restart
	make docker-logs-f

docker-clean:
	make docker-stop || true
	docker-compose rm -f

docker-clean-start:
	docker-compose down
	make docker-build
	make docker-start
	echo
	echo "waiting 5 seconds to let services start"
	sleep 5
	echo
	make docker-logs-f

docker-stop:
	docker-compose stop

docker-push:
	docker push orihoch/mojp-dbs-pipelines

deploy:
	git pull origin $(DEPLOY_BRANCH)
	make docker-build
	make docker-start
