## Docker README

Docker is used to run all the services and apps in a consistent way for both development and production.

**For most changes you can just run the code directly / use the unit tests - no need to run the Docker**

### Installation (commands are for Ubuntu 17.04)

* [Install Docker](https://docs.docker.com/engine/installation/) (tested with version 17.03)
  * `sudo apt-get remove docker docker-engine docker-compose docker.io`
  * `sudo apt-get install apt-transport-https ca-certificates curl software-properties-common`
  * `curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -`
  * `sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu xenial stable"`
  * `sudo apt-get update`
  * `sudo apt-get install docker-ce`
* [Install Docker Compose](https://docs.docker.com/compose/install/)
  * ```curl -L https://github.com/docker/compose/releases/download/1.13.0/docker-compose-`uname -s`-`uname -m` | sudo tee /usr/local/bin/docker-compose > /dev/null```
  * `sudo chmod +x /usr/local/bin/docker-compose`
* fix Docker permissions
  * `sudo usermod -aG docker $USER`
  * `sudo su -l $USER`
  * `docker ps`
  * restart to make sure group change takes effect
* clean start
  * `make docker-clean-start`

### Modifying configurations

* `cp docker-compose.override.yml.example docker-compose.override.yml`
* modify docker-compose.override.yml
* you can override all settings in docker-compose.yml

### Connecting to local ElasticSearch instance for development

If you have a local ES you want to test with:

* Find out the docker network
  * On Ubuntu 17.04:
    * `sudo ifconfig -a`
    * should get something like this:
      * `docker0: flags=4099<UP,BROADCAST,MULTICAST>  mtu 1500`
      * `inet 172.17.0.1  netmask 255.255.0.0  broadcast 0.0.0.0`
    * the relevant host IP is 172.17.0.1
* Expose Elasticsearch to the docker network
  * edit /etc/elasticsearch/elasticsearch.yml
    * change the network.host:
      * `network.host:`
      * `  - _local_`
      * `  - 172.17.0.1`
* modify MOJP_ELASTICSEARCH_DB in the docker-compose.override.yml:
   * `"MOJP_ELASTICSEARCH_DB=172.17.0.1"`

### Modifying code and restarting the app

In the docker-compose.override - uncomment the volumes directive - this will mount the host directory inside docker

Now, every change in the code is reflected inside docker

Restart the app with `make docker-restart`

for a full rebuild and restart run: `make docker-clean-start`

### debugging

```
docker exec -it mojpdbspipelines_app_1 sh
dpp
```

You can also copy the data directory from inside docker - to get the sync log

```
docker cp mojpdbspipelines_app_1:/mojp/data mojpdbspipelines_data
```
