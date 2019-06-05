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
import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration test for InfluxDB. Please ensure that the InfluxDB is running
 * on local host with default port and has database test with table test. Please set user
 * and password if applicable before running the integration tests.
 */
public class ITPutInfluxDatabase extends AbstractITInfluxDB {

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutInfluxDatabase.class);
        initializeRunner();
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY, DEFAULT_RETENTION_POLICY);
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
        initInfluxDB();
    }

    @Test
    public void testValidSinglePoint() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plusNanos(1), timestamp);
    }

    @Test
    public void testPrecisionNanoseconds() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.NANOSECONDS.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.NANOS), timestamp);
    }


    @Test
    public void testPrecisionMicroseconds() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.MICROSECONDS.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MICROS), timestamp);
    }

    @Test
    public void testPrecisionMilliseconds() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.MILLISECONDS.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MILLIS), timestamp);
    }

    @Test
    public void testPrecisionSeconds() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.SECONDS.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.SECONDS), timestamp);
    }

    @Test
    public void testPrecisionMinutes() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.MINUTES.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MINUTES), timestamp);
    }

    @Test
    public void testPrecisionHours() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());
        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());

        Instant timestamp = Instant.parse(values.get(0).get(0).toString());

        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS), timestamp);
    }

    @Test
    public void testValidSinglePointWithTime() {
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("Should have no results", null, result.getResults().iterator().next().getSeries());
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());
    }

    @Test
    public void testValidSinglePointWithTimeAndUrlExpression() {
        runner.setVariable("influxDBUrl", "http://localhost:8086");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "${influxDBUrl}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("Should have no results", null, result.getResults().iterator().next().getSeries());
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());
   }

    @Test
    public void testValidSinglePointWithUsernameEL() {
        runner.setVariable("influxdb.username", "admin");
        runner.setProperty(PutInfluxDatabase.USERNAME, "${influxdb.username}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("Should have no results", null, result.getResults().iterator().next().getSeries());
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
   }

    @Test
    public void testValidSinglePointWithPasswordEL() {
        runner.setVariable("influxdb.password", "admin");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "${influxdb.password}");
        QueryResult result = influxDB.query(new Query("select * from water where time = 1501002274856668652", dbName));
        assertEquals("Should have no results", null, result.getResults().iterator().next().getSeries());
        String message = "water,country=US,city=sf rain=1,humidity=0.6 1501002274856668652";
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
   }

   @Test
    public void testValidTwoPointWithSameMeasurement() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "water,country=US,city=nyc rain=2,humidity=0.7" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 2, values.size());
    }

    @Test
    public void testValidTwoPointWithSameMeasurementBadFormat() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "water,country=US,city=nyc,rain=2,humidity=0.7" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_FAILURE);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal","partial write: unable to parse 'water,country=US,city=nyc,rain=2,humidity=0.7': missing fields dropped=0",
            flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water", dbName));
        assertEquals("size should be same", 1, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());
    }

    @Test
    public void testValidTwoPointWithDifferentMeasurement() {
        String message = "water,country=US,city=newark rain=1,humidity=0.6" + System.lineSeparator()
                + "testm,country=US,city=chicago rain=10,humidity=0.9" + System.lineSeparator();
        byte [] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals("Value should be equal", 1, flowFiles.size());
        assertEquals("Value should be equal",null, flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE));
        QueryResult result = influxDB.query(new Query("select * from water, testm", dbName));
        assertEquals("size should be same", 2, result.getResults().iterator().next().getSeries().size());
        List<List<Object>> values = result.getResults().iterator().next().getSeries().iterator().next().getValues();
        assertEquals("size should be same", 1, values.size());
    }
}