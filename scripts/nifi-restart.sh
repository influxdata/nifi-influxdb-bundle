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

#!/usr/bin/env bash

set -e

#### wait to NiFi startup
function waitNifiStarted {
	echo "Waiting to Nifi: $1"
	repeat=0;
	timeout=200
	until $(curl --output /dev/null --silent --head --fail ${1}); do
	    printf '.'
	    ((repeat++))
	    if ((repeat > timeout)) ; then
	        echo "Server $1 is not alive!"
	        exit 1;
        fi
	    sleep 1
	done
	echo "." && echo "NiFi $1 is online"
}


DEFAULT_INFLUXDB_VERSION="1.7"
INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"
INFLUXDB_IMAGE=influxdb:${INFLUXDB_VERSION}-alpine

DEFAULT_NIFI_VERSION="1.8.0"
NIFI_VERSION="${NIFI_VERSION:-$DEFAULT_NIFI_VERSION}"
NIFI_IMAGE=apache/nifi:${NIFI_VERSION}

DEFAULT_TELEGRAF_VERSION="1.9"
TELEGRAF_VERSION="${TELEGRAF_VERSION:-$DEFAULT_TELEGRAF_VERSION}"
TELEGRAF_IMAGE=telegraf:${TELEGRAF_VERSION}

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

echo "Building nifi-influxdata-nar..."

cd ${SCRIPT_PATH}/..

mvn -B clean install -DskipTests

echo
echo "Stoping Docker containers..."
echo

docker kill telegraf || true
docker rm telegraf || true

docker kill nifi || true
docker rm nifi || true

docker kill influxdb || true
docker rm influxdb || true

echo
echo "Starting InfluxDB..."
echo

docker run \
          --detach \
          --name influxdb \
          --publish 8086:8086 \
          --publish 8089:8089/udp \
          --volume ${SCRIPT_PATH}/../nifi-influx-database-services/src/test/resources/influxdb.conf:/etc/influxdb/influxdb.conf \
      ${INFLUXDB_IMAGE}

sleep 5

#
# Create database for Twitter demo
#

docker exec -ti influxdb sh -c "influx -execute 'create database twitter_demo'"
docker exec -ti influxdb sh -c "influx -execute 'create database telegraf_nifi_demo'"

echo
echo "Build Apache NiFi with demo..."
echo

gzip < ${SCRIPT_PATH}/flow.xml > ${SCRIPT_PATH}/flow.xml.gz
# docker cp nifi:/opt/nifi/nifi-current/conf/flow.xml.gz scripts/
# gunzip -f scripts/flow.xml.gz

docker build -t nifi -f ${SCRIPT_PATH}/Dockerfile --build-arg NIFI_IMAGE=${NIFI_IMAGE} .

echo
echo "Starting Apache NiFi..."
echo

docker run \
    --detach \
    --name nifi \
    --publish 8080:8080 \
	--publish 8007:8000 \
	--publish 6666:6666 \
	--link=influxdb \
	nifi

waitNifiStarted "http://localhost:8080/nifi/"

echo
echo "Starting Telegraf..."
echo

docker run \
    --detach \
    --name=telegraf \
    --net=container:nifi \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v ${SCRIPT_PATH}/telegraf.conf:/etc/telegraf/telegraf.conf:ro \
    ${TELEGRAF_IMAGE}