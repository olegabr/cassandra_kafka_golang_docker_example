# cassandra_kafka_golang_docker_example
The exmple code that uses cassandra, kafka and docker to build a simple user service.

docker install
==============

```
apt-get update
apt-get upgrade -y

# install docker
if ! hash docker 2>/dev/null; then
        curl -fsSL https://get.docker.com/ | sh
fi
# install docker-compose
if ! hash docker-compose 2>/dev/null; then
        curl -L https://github.com/docker/compose/releases/download/1.8.1/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose
fi
```

Build go-service
================

```
docker-compose build
```

Startup go-service and other services
=====================================

```
docker-compose up -d
```

See logs (optional)
===================

```
docker-compose logs -tf
```

Init database
=============

After about a minute cassandra complete it's initialization and you can enter to it's docker-container:

```
docker exec -it cv_cassandra_1 bash
```

Inside the container you can enter the CQL shell:

```
cqlsh
```

Use this command to create a keyspace:

```
CREATE KEYSPACE thegame WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3} AND DURABLE_WRITES = true;
```

Leave the shell and docker-container: `Ctrl+d`

If you see logs again:

```
docker-compose logs -tf
```

You should see line similar to this one:

```
user_create_worker_1  | 2016-12-13T17:52:54.772716495Z Found Offset:	-1
```

It means that our user service successfully connected to the database and queue and is ready to work.

