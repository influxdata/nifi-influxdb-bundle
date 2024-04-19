/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.influxdata.nifi.util;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.query.FluxTable;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ITTestTimeout {
    static final String INFLUX_DB_2_URL = "http://localhost:9999";
    static final String INFLUX_DB_2_ORG = "my-org";
    static final String INFLUX_DB_2_TOKEN = "my-token";

    @Test
    public void testReadTimeout() {
        InfluxDBClient influxDBClient = InfluxDBUtils.makeConnectionV2(INFLUX_DB_2_URL, INFLUX_DB_2_TOKEN, 60, null, null);
        List<FluxTable> unused = influxDBClient.getQueryApi().query("import \"array\"\n" +
                "import \"experimental/json\"\n" +
                "import \"http/requests\"\n" +
                "response = requests.get(url: \"http://httpbin:8080/delay/20\")\n" +
                "data = json.parse(data: response.body)\n" +
                "array.from(rows: [{_field: \"origin\", _value: data.origin, _time: now()}])", INFLUX_DB_2_ORG);
    }
}
