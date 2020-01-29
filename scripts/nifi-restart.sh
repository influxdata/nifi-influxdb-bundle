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


set -e

DEFAULT_INFLUXDB_VERSION="1.7"
INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"
INFLUXDB_IMAGE=influxdb:${INFLUXDB_VERSION}-alpine

DEFAULT_NIFI_VERSION="1.11.0"
NIFI_VERSION="${NIFI_VERSION:-$DEFAULT_NIFI_VERSION}"
NIFI_IMAGE=apache/nifi:${NIFI_VERSION}

DEFAULT_TELEGRAF_VERSION="1.9"
TELEGRAF_VERSION="${TELEGRAF_VERSION:-$DEFAULT_TELEGRAF_VERSION}"
TELEGRAF_IMAGE=telegraf:${TELEGRAF_VERSION}

DEFAULT_CHRONOGRAF_VERSION="1.7"
CHRONOGRAF_VERSION="${CHRONOGRAF_VERSION:-$DEFAULT_CHRONOGRAF_VERSION}"
CHRONOGRAF_IMAGE=chronograf:${CHRONOGRAF_VERSION}

DEFAULT_INFLUXDB_V2_VERSION="nightly"
INFLUXDB_V2_VERSION="${INFLUXDB_V2_VERSION:-$DEFAULT_INFLUXDB_V2_VERSION}"
INFLUXDB_V2_IMAGE=quay.io/influxdb/influx:${INFLUXDB_V2_VERSION}

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

echo "Building nifi-influxdata-nar..."

cd ${SCRIPT_PATH}/..

mvn -B clean install -DskipTests

echo
echo "Stoping Docker containers..."
echo

docker kill chronograf || true
docker rm chronograf || true

docker kill telegraf || true
docker rm telegraf || true

docker kill kafka || true
docker rm kafka || true

docker kill nifi || true
docker rm nifi || true

docker kill influxdb || true
docker rm influxdb || true

docker kill influxdb_v2 || true
docker rm influxdb_v2 || true

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

echo "Wait to start InfluxDB"
wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:8086/ping

#
# InfluxDB 2.0
#
echo
echo "Starting InfluxDB 2.0 [${INFLUXDB_V2_IMAGE}] ... "
echo

docker pull ${INFLUXDB_V2_IMAGE} || true
docker run \
       --detach \
       --name influxdb_v2 \
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

#
# Create database for Twitter demo
#

docker exec -ti influxdb sh -c "influx -execute 'create database twitter_demo'"
docker exec -ti influxdb sh -c "influx -execute 'create database telegraf_nifi_demo'"
docker exec -ti influxdb sh -c "influx -execute 'create database kafka_influxdb_demo'"

echo
echo "Starting Chronograf..."
echo

docker run \
    --detach \
    --name chronograf \
    --publish 8888:8888 \
	--link=influxdb \
	${CHRONOGRAF_IMAGE} --influxdb-url=http://influxdb:8086


echo
echo "Starting Kafka..."
echo

docker run \
    --detach \
    --name kafka \
    --publish 2181:2181 \
    --publish 9092:9092 \
    --env ADVERTISED_PORT=9092 \
	spotify/kafka

echo
echo "Build Apache NiFi with demo..."
echo

ORGID=$(docker exec -it influxdb_v2 influx org find -t my-token | grep my-org  | awk '{ print $1 }')
cp "${SCRIPT_PATH}"/flow.xml "${SCRIPT_PATH}"/flow.edited.xml
sed -i.backup "s/influxdb-org-replacement-id/${ORGID}/g" "${SCRIPT_PATH}"/flow.edited.xml
gzip < "${SCRIPT_PATH}"/flow.edited.xml > "${SCRIPT_PATH}"/flow.xml.gz

# docker cp nifi:/opt/nifi/nifi-current/conf/flow.xml.gz scripts/ && gunzip -f scripts/flow.xml.gz
# gunzip -f scripts/flow.xml.gz

docker build -t nifi -f "${SCRIPT_PATH}"/Dockerfile --build-arg NIFI_IMAGE="${NIFI_IMAGE}" .

echo
echo "Starting Apache NiFi..."
echo

docker run \
    --detach \
    --name nifi \
    --publish 8080:8080 \
	--publish 8007:8000 \
	--publish 6666:6666 \
	--publish 8123:8123 \
	--publish 8234:8234 \
	--link=influxdb \
	--link=influxdb_v2 \
	--link=kafka \
	nifi

wget -S --spider --tries=20 --retry-connrefused --waitretry=5 http://localhost:8080/nifi/

echo
echo "Starting Telegraf..."
echo

docker run \
    --detach \
    --name=telegraf \
    --net=container:nifi \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${SCRIPT_PATH}"/telegraf.conf:/etc/telegraf/telegraf.conf:ro \
    ${TELEGRAF_IMAGE}

echo
echo "Import Chronograf settings..."
echo

curl -i -X POST -H "Content-Type: application/json" http://localhost:8888/chronograf/v1/dashboards -d @${SCRIPT_PATH}/chronograf/nifi-dashboard.json
curl -i -X POST -H "Content-Type: application/json" http://localhost:8888/chronograf/v1/dashboards -d @${SCRIPT_PATH}/chronograf/twitter-dashboard.json
curl -i -X POST -H "Content-Type: application/json" http://localhost:8888/chronograf/v1/dashboards -d @${SCRIPT_PATH}/chronograf/nifi-logs-dashboard.json

#curl -i -X POST -H "Content-Type: application/json" -H "Authorization: Token my-password" http://localhost:9999/api/v2/dashboards  -d @scripts/influx2/nifi_dashboard.json
