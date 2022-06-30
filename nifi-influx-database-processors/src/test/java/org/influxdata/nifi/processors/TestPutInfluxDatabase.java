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

import java.io.EOFException;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPutInfluxDatabase {
    private TestRunner runner;
    private PutInfluxDatabase mockPutInfluxDatabase;
    private TimeUnit precision;

    @Before
    public void setUp() {
        precision = null;
        mockPutInfluxDatabase = new PutInfluxDatabase() {
            @Override
            protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout, final String clientType) {
                return null;
            }

            @Override
            protected void writeToInfluxDB(ProcessContext context, String consistencyLevel, String database, String retentionPolicy,
                                           final TimeUnit precision, String records) {
                TestPutInfluxDatabase.this.precision = precision;

            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "user");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "password");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testBlankDBUrl() {
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyDBName() {
        runner.setProperty(PutInfluxDatabase.DB_NAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyConnectionTimeout() {
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_CONNECTION_TIMEOUT, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyUsername() {
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "password");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
        runner.setProperty(PutInfluxDatabase.USERNAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyPassword() {
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "username");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
        runner.setProperty(PutInfluxDatabase.PASSWORD, "");
        runner.assertNotValid();
    }

    @Test
    public void testPasswordEL() {
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setVariable("influxdb.password", "password");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "${influxdb.password}");
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "username");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
    }

    @Test
    public void testUsernameEL() {
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setVariable("influxdb.username", "username");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "password");
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "${influxdb.username}");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
    }

    @Test
    public void testCharsetUTF8() {
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.assertValid();
    }

    @Test
    public void testEmptyConsistencyLevel() {
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, "");
        runner.assertNotValid();
    }

    @Test
    public void testCharsetBlank() {
        runner.setProperty(PutInfluxDatabase.CHARSET, "");
        runner.assertNotValid();
    }
    @Test
    public void testZeroMaxDocumentSize() {
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testSizeGreaterThanThresholdUsingEL() {
        runner.setVariable("max.record.size", "1 B");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "${max.record.size}");
        runner.assertValid();
        byte [] bytes = new byte[2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_MAX_SIZE_EXCEEDED, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_MAX_SIZE_EXCEEDED);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"Max records size exceeded " + bytes.length);
    }

    @Test
    public void testSizeGreaterThanThreshold() {
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 B");
        runner.assertValid();
        byte [] bytes = new byte[2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_MAX_SIZE_EXCEEDED, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_MAX_SIZE_EXCEEDED);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"Max records size exceeded " + bytes.length);
    }

    @Test
    public void testValidSingleMeasurement() {
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 MB");
        runner.assertValid();
        byte [] bytes = "test".getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE), null);
        Assert.assertNull(precision);
    }

    @Test
    public void testPrecision() {
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 MB");
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.SECONDS.name());
        runner.assertValid();
        byte [] bytes = "test".getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE), null);
        Assert.assertEquals(TimeUnit.SECONDS, precision);
    }

    @Test
    public void testPrecisionDays() {
        runner.setProperty(PutInfluxDatabase.TIMESTAMP_PRECISION, TimeUnit.DAYS.name());
        runner.assertNotValid();
    }

    @Test
    public void testWriteThrowsException() {
        mockPutInfluxDatabase = new PutInfluxDatabase() {
            @Override
            protected void writeToInfluxDB(ProcessContext context, String consistencyLevel, String database, String retentionPolicy,
                                           final TimeUnit precision, String records) {
                throw new RuntimeException("WriteException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "u1");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "p1");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"WriteException");
    }

    @Test
    public void testWriteThrowsIOException() {
        mockPutInfluxDatabase = new PutInfluxDatabase() {
            @Override
            protected void writeToInfluxDB(ProcessContext context, String consistencyLevel, String database, String retentionPolicy,
                                           final TimeUnit precision, String records) {
                throw new InfluxDBIOException(new EOFException("EOFException"));
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "u1");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "p1");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"java.io.EOFException: EOFException");
    }

    @Test
    public void testWriteThrowsSocketTimeoutException() {
        mockPutInfluxDatabase = new PutInfluxDatabase() {
            @Override
            protected void writeToInfluxDB(ProcessContext context, String consistencyLevel, String database, String retentionPolicy,
                                           final TimeUnit precision, String records) {
                throw new InfluxDBIOException(new SocketTimeoutException("SocketTimeoutException"));
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "u1");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "p1");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_RETRY);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"java.net.SocketTimeoutException: SocketTimeoutException");
    }

    @Test
    public void testTriggerThrowsException() {
        mockPutInfluxDatabase = new PutInfluxDatabase() {
            @Override
            protected InfluxDB getInfluxDB(ProcessContext context) {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDatabase);
        runner.setProperty(PutInfluxDatabase.DB_NAME, "test");
        runner.setProperty(PutInfluxDatabase.USERNAME, "u1");
        runner.setProperty(PutInfluxDatabase.PASSWORD, "p1");
        runner.setProperty(PutInfluxDatabase.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDatabase.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDatabase.CONSISTENCY_LEVEL, PutInfluxDatabase.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDatabase.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE),"testException");
    }

    @Test
    public void testValidArrayMeasurement() {
        runner.setProperty(PutInfluxDatabase.MAX_RECORDS_SIZE, "1 MB");
        runner.assertValid();

        runner.enqueue("test rain=2\ntest rain=3".getBytes());
        runner.run(1,true,true);

        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE), null);
    }

    @Test
    public void testInvalidEmptySingleMeasurement() {
        byte [] bytes = "".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDatabase.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDatabase.INFLUX_DB_ERROR_MESSAGE), "Empty measurement size 0");
    }

}