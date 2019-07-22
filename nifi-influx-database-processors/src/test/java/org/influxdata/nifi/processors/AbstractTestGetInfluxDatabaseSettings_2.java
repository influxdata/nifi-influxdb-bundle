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
package org.influxdata.nifi.processors;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.influxdata.client.InfluxDBClient;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.mockito.Mockito;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;

/**
 * @author Jakub Bednar (bednar@github) (22/07/2019 08:11)
 */
abstract class AbstractTestGetInfluxDatabaseSettings_2 {

    TestRunner runner;

    @Before
    public void before() throws IOException, GeneralSecurityException, InitializationException {

        InfluxDBClient mockInfluxDBClient = Mockito.mock(InfluxDBClient.class);

        GetInfluxDatabase_2 processor = Mockito.spy(new GetInfluxDatabase_2());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(GetInfluxDatabase_2.ORG, "my-org");
        runner.setProperty(GetInfluxDatabase_2.QUERY, "from(bucket:\"my-bucket\") |> range(start: 0) |> last()");
        runner.setProperty(GetInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }
}