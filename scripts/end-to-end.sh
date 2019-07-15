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

echo
echo "Querying data by 'select count(tweet_id) from tweets'"
echo

twitter=`(curl -s 'http://localhost:8086/query' --data-urlencode "db=twitter_demo" --data-urlencode "q=select count(tweet_id) from tweets")`

echo "> result:" ${twitter}

echo ${twitter} | jq '.results[0].series[0].values[0][1] > 0' | grep -q 'true'
if [[ $? -eq 0 ]]; then
    echo "> check: success"
else
    echo "> check: fail..."  `(echo ${twitter} | jq '.results[0].series[0].values[0][1]')`
    exit 1
fi

echo
echo "Querying data by 'select count(pid) from docker_container_status'"
echo

docker_container_status=`(curl -s 'http://localhost:8086/query' --data-urlencode "db=telegraf_nifi_demo" --data-urlencode "q=select count(pid) from docker_container_status")`

echo "Query result: " ${docker_container_status}

echo ${docker_container_status} | jq '.results[0].series[0].values[0][1] > 0' | grep -q 'true'
if [[ $? -eq 0 ]]; then
    echo "> check: success"
else
    echo "> check: fail..."  `(echo ${docker_container_status} | jq '.results[0].series[0].values[0][1]')`
    exit 1
fi

echo
echo "Querying data by 'SELECT count(message) AS count_message FROM kafka_influxdb_demo.autogen.nifi_logs'"
echo

nifi_logs=`(curl -s 'http://localhost:8086/query' --data-urlencode "db=telegraf_nifi_demo" --data-urlencode "q=SELECT count(message) AS count_message FROM kafka_influxdb_demo.autogen.nifi_logs")`

echo "Query result: " ${nifi_logs}

echo ${nifi_logs} | jq '.results[0].series[0].values[0][1] > 0' | grep -q 'true'
if [[ $? -eq 0 ]]; then
    echo "> check: success"
else
    echo "> check: fail..."  `(echo ${nifi_logs} | jq '.results[0].series[0].values[0][1]')`
    exit 1
fi

echo
echo "Querying from InfluxDB 2.0 data by:"
echo "  from(bucket: "my-bucket")"
echo "      |> range(start: 0)"
echo "      |> filter(fn: (r) => r._measurement == "nifi_logs")"
echo "      |> filter(fn: (r) => r._field == "message")"
echo "      |> drop(columns: ["thread", "level"])"
echo "      |> count()"
echo

nifi_logs_v2=`(curl -s POST \
  http://localhost:9999/api/v2/query?org=my-org \
  -H 'Authorization: Token my-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "from(bucket:\"my-bucket\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \"nifi_logs\") |> filter(fn: (r) => r._field == \"message\") |> drop(columns: [\"thread\", \"level\"]) |> count()",
    "dialect" : {
        "header": false,
        "annotations": []
    }
}')`

nifi_logs_v2_count=`(echo ${nifi_logs_v2} | head | cut -d ',' -f 8 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')`
echo ${nifi_logs_v2_count}
if [[ ${nifi_logs_v2_count} -gt 0 ]]; then
    echo "> check: success"
else
    echo "> check: fail..."  ${nifi_logs_v2}
    exit 1
fi