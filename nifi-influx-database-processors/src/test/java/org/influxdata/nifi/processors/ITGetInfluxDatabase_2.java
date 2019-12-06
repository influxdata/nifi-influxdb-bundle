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

import java.util.Arrays;

import com.influxdb.client.domain.WritePrecision;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_URL;

/**
 * @author Jakub Bednar (23/07/2019 11:32)
 */
public class ITGetInfluxDatabase_2 extends AbstractITInfluxDB_2 {

    @Before
    public void setUp() throws Exception {

        init();

        GetInfluxDatabase_2 putInfluxDatabase_2 = new GetInfluxDatabase_2();
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
        runner.setProperty(GetInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(GetInfluxDatabase_2.ORG, organization.getId());
        runner.setProperty(GetInfluxDatabase_2.QUERY, "from(bucket:\"" + bucketName + "\") |> range(start: 0)");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_URL, INFLUX_DB_2);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }

    @Test
    public void oneRow() {

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabase_2.REL_SUCCESS, 1);

        String data = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabase_2.REL_SUCCESS).get(0))).trim();
        Assert.assertTrue(data, data.startsWith(",_result,0,1970-01-01T00:00:00Z,"));
        Assert.assertTrue(data, data.endsWith("1970-01-01T00:00:00.000000001Z,0.6,humidity,water,newark,US"));
    }

    @Test
    public void moreRows() {

        influxDBClient.getWriteApiBlocking().writeRecords(WritePrecision.NS, Arrays.asList(
                "water,country=US,city=newark humidity=0.1 1",
                "water,country=US,city=newark humidity=0.2 2",
                "water,country=US,city=newark humidity=0.3 3",
                "water,country=US,city=newark humidity=0.4 4",
                "water,country=US,city=newark humidity=0.5 5",
                "water,country=US,city=newark humidity=0.6 6"
        ));

        runner.setProperty(GetInfluxDatabase_2.RECORDS_PER_FLOWFILE, "2");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabase_2.REL_SUCCESS, 3);

        String data = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabase_2.REL_SUCCESS).get(0))).trim();
        Assert.assertTrue(data, data.startsWith(",_result,0,1970-01-01T00:00:00Z,"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000001Z,0.1,humidity,water,newark,US"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000002Z,0.2,humidity,water,newark,US"));

        data = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabase_2.REL_SUCCESS).get(1))).trim();
        Assert.assertTrue(data, data.startsWith(",_result,0,1970-01-01T00:00:00Z,"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000003Z,0.3,humidity,water,newark,US"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000004Z,0.4,humidity,water,newark,US"));

        data = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabase_2.REL_SUCCESS).get(2))).trim();
        Assert.assertTrue(data, data.startsWith(",_result,0,1970-01-01T00:00:00Z,"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000005Z,0.5,humidity,water,newark,US"));
        Assert.assertTrue(data, data.contains("1970-01-01T00:00:00.000000006Z,0.6,humidity,water,newark,US"));
    }

    @Test
    public void invalidFlux() {

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.setProperty(GetInfluxDatabase_2.QUERY, "from(bucket:\"" + bucketName + "\") |> rangex(start: 0)");

        runner.run(1);

        runner.assertTransferCount(GetInfluxDatabase_2.REL_SUCCESS, 0);
        runner.assertTransferCount(GetInfluxDatabase_2.REL_RETRY, 0);
        runner.assertTransferCount(GetInfluxDatabase_2.REL_FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_FAILURE).get(0);
        Assert.assertEquals("type error 1:45-1:51: undefined identifier \"rangex\"", flowFile.getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void wrongParameters() {

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.setProperty(GetInfluxDatabase_2.ORG, "not-exists");

        runner.run(1);

        runner.assertTransferCount(GetInfluxDatabase_2.REL_SUCCESS, 0);
        runner.assertTransferCount(GetInfluxDatabase_2.REL_RETRY, 0);
        runner.assertTransferCount(GetInfluxDatabase_2.REL_FAILURE, 1);
    }
}
