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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.influxdata.client.domain.WritePrecision;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.influxdata.query.FluxTable;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_URL;
import static org.junit.Assert.assertEquals;

/**
 * @author Jakub Bednar (bednar@github) (15/07/2019 09:05)
 */
public class ITPutInfluxDatabase_2 extends AbstractITInfluxDB_2 {

    @Before
    public void setUp() throws Exception {

        init();

        PutInfluxDatabase_2 putInfluxDatabase_2 = new PutInfluxDatabase_2();
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, bucketName);
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_URL, INFLUX_DB_2);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }

    @Test
    public void testValid() {
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0,1), tables.get(0).getRecords().get(0).getTime());
    }
    
    @Test
    public void testBadFormat() {
        runner.enqueue("water,country=US,city=newark".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        assertEquals(1, flowFiles.size());
        assertEquals("unable to parse points: unable to parse 'water,country=US,city=newark': missing fields",
                flowFiles.get(0).getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void testPrecisionNanosecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.NS.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0,1), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionMicrosecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.US.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MICROS), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionMillisecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.MS.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MILLIS), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionSecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.S.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.SECONDS), tables.get(0).getRecords().get(0).getTime());
    }
}