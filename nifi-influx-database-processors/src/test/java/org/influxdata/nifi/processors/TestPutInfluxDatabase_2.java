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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Jakub Bednar (bednar@github) (11/07/2019 09:38)
 */
public class TestPutInfluxDatabase_2 {
    private TestRunner runner;
    private InfluxDBClient mockInfluxDBClient;
    private PutInfluxDatabase_2 mockPutInfluxDatabase;

    @Before
    public void setUp() throws InitializationException, IOException, GeneralSecurityException {
        mockInfluxDBClient = Mockito.mock(InfluxDBClient.class);
        mockPutInfluxDatabase = new PutInfluxDatabase_2();
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "my-bucket");
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testEmptyDBService() {
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyBucket() {
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "");
        runner.assertNotValid();
    }
    
    @Test
    public void testEmptyOrg() {
        runner.setProperty(PutInfluxDatabase_2.ORG, "");
        runner.assertNotValid();
    }

    @Test
    public void testPrecisionAllowedValues() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "MS");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "S");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "US");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "NS");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "D");
        runner.assertNotValid();
    }

    @After
    public void tearDown() {
        runner = null;
    }
}