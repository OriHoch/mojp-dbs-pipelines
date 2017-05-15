from setuptools import setup, find_packages
import os, time

if os.path.exists("VERSION.txt"):
    # this file can be written by CI tools (e.g. Travis)
    with open("VERSION.txt") as version_file:
        version = version_file.read().strip().strip("v")
else:
    version = str(time.time())

# Run
setup(
    name="datapackage-pipelines-mojp",
    version=version,
    packages=find_packages(exclude=["tests", "test.*"]),
    install_requires=["datapackage-pipelines",
                      "python-dotenv",
                      "elasticsearch",
                      "pyquery"],
    extras_require={'develop': ["tox"]},
    url='https://github.com/beit-Hatfutsot/mojp-dbs-pipelines',
    license='MIT',
)
