#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#
# The script start two instances of InfluxDB. The first is unsecured and is reachable on http://localhost:8086.
# The second is secured by HTTPS and is reachable on https://localhost:9086
#
set -e

DEFAULT_DOCKER_REGISTRY="quay.io/influxdb/"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-$DEFAULT_DOCKER_REGISTRY}"

DEFAULT_INFLUXDB_VERSION="1.8"
INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"
INFLUXDB_IMAGE=influxdb:${INFLUXDB_VERSION}-alpine

DEFAULT_INFLUXDB_V2_REPOSITORY="influxdb"
DEFAULT_INFLUXDB_V2_VERSION="v2.0.3"
INFLUXDB_V2_REPOSITORY="${INFLUXDB_V2_REPOSITORY:-$DEFAULT_INFLUXDB_V2_REPOSITORY}"
INFLUXDB_V2_VERSION="${INFLUXDB_V2_VERSION:-$DEFAULT_INFLUXDB_V2_VERSION}"
INFLUXDB_V2_IMAGE=${DOCKER_REGISTRY}${INFLUXDB_V2_REPOSITORY}:${INFLUXDB_V2_VERSION}

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

docker kill influxdb || true
docker rm influxdb || true

docker kill influxdb-secured || true
docker rm influxdb-secured || true

docker kill influxdb_v2 || true
docker rm influxdb_v2 || true

docker pull ${INFLUXDB_IMAGE} || true
docker pull ${INFLUXDB_V2_IMAGE} || true

echo
echo "Starting unsecured InfluxDB..."
echo

docker run \
          --detach \
          --name influxdb \
          --publish 8086:8086 \
          --publish 8089:8089/udp \
          --volume ${SCRIPT_PATH}/../nifi-influx-database-services/src/test/resources/influxdb.conf:/etc/influxdb/influxdb.conf \
      ${INFLUXDB_IMAGE}

echo "Wait to start InfluxDB"
wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:8086/ping

echo
echo "Starting secured InfluxDB..."
echo

docker run \
          --detach \
          --name influxdb-secured \
          --publish 9086:9086 \
          --publish 9089:9089/udp \
          --volume ${SCRIPT_PATH}/../nifi-influx-database-services/src/test/resources/influxdb-secured.conf:/etc/influxdb/influxdb.conf \
          --volume ${SCRIPT_PATH}/../nifi-influx-database-services/src/test/resources/ssl/influxdb-selfsigned.crt:/etc/influxdb/influxdb-selfsigned.crt \
          --volume ${SCRIPT_PATH}/../nifi-influx-database-services/src/test/resources/ssl/influxdb-selfsigned.key:/etc/influxdb/influxdb-selfsigned.key \
      ${INFLUXDB_IMAGE}


#
# InfluxDB 2.0
#
echo
echo "Starting InfluxDB 2.0 [${INFLUXDB_V2_IMAGE}] ... "
echo

docker run \
       --detach \
       --env INFLUXD_HTTP_BIND_ADDRESS=:9999 \
       --name influxdb_v2 \
       --link=influxdb \
       --publish 9999:9999 \
       ${INFLUXDB_V2_IMAGE}

echo "Wait to start InfluxDB 2.0"
wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:9999/metrics

echo
echo "Post onBoarding request, to setup initial user (my-user@my-password), org (my-org) and bucket (my-bucket)"
echo
curl -i -X POST http://localhost:9999/api/v2/setup -H 'accept: application/json' \
    -d '{
            "username": "my-user",
            "password": "my-password",
            "org": "my-org",
            "bucket": "my-bucket",
            "token": "my-token"
        }'

docker ps
