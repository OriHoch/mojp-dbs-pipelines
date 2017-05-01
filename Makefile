.PHONY: install test clean

install:
	pip install --upgrade pip setuptools
	pip install --upgrade -e .[develop]

test:
	tox -r

clean:
	pip uninstall -y mojp_dbs_pipelines || true
	pip freeze | xargs pip uninstall -y || true
