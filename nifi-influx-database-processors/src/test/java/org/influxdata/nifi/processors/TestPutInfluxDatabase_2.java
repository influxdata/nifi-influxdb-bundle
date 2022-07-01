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
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.processors.Utils.createErrorResponse;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.CHARSET;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_RETRY_AFTER;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_FAILURE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_MAX_SIZE_EXCEEDED;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_RETRY;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_SUCCESS;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Jakub Bednar (bednar@github) (11/07/2019 09:38)
 */
public class TestPutInfluxDatabase_2 {
    private TestRunner runner;
    private InfluxDBClient mockInfluxDBClient;
    private WriteApiBlocking mockWriteApi;

    @Before
    public void setUp() throws InitializationException, IOException, GeneralSecurityException {
        mockWriteApi = Mockito.mock(WriteApiBlocking.class);
        mockInfluxDBClient = Mockito.mock(InfluxDBClient.class);
        Mockito.doAnswer(invocation -> mockWriteApi).when(mockInfluxDBClient).getWriteApiBlocking();

        PutInfluxDatabase_2 putInfluxDatabase_2 = new PutInfluxDatabase_2();
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "my-bucket");
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }

    @After
    public void tearDown() {
        runner = null;
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

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "DAYS");
        runner.assertNotValid();
    }

    @Test
    public void testCharsetUTF8() {
        runner.setProperty(CHARSET, "UTF-8");
        runner.assertValid();
    }

    @Test
    public void testCharsetBlank() {
        runner.setProperty(CHARSET, "");
        runner.assertNotValid();
    }

    @Test
    public void testMaxRecordSizeZero() {
        runner.setProperty(MAX_RECORDS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testMaxRecordSizeOverThreshold() {
        runner.setProperty(MAX_RECORDS_SIZE, "1 B");
        runner.assertValid();

        byte[] bytes = "aa".getBytes();
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_MAX_SIZE_EXCEEDED, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_MAX_SIZE_EXCEEDED);
        assertEquals(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE), "Max records size exceeded " + bytes.length);
    }

    @Test
    public void testMaxRecordSizeOverThresholdEL() {
        runner.setVariable("max.record.size", "1 B");
        runner.setProperty(MAX_RECORDS_SIZE, "${max.record.size}");
        runner.assertValid();

        byte[] bytes = "aa".getBytes();
        runner.enqueue(bytes);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_MAX_SIZE_EXCEEDED, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_MAX_SIZE_EXCEEDED);
        assertEquals(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE), "Max records size exceeded " + bytes.length);
    }

    @Test
    public void testWriteEmpty() {

        byte[] bytes = "".getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE),"Empty measurements");
    }

    @Test
    public void testWriteThrowsException() {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        Mockito.doThrow(new RuntimeException("WriteException")).when(mockWriteApi)
                .writeRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE),"WriteException");
    }

    @Test
    public void testWriteSocketTimeoutExceptionRetry() {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        Mockito.doThrow(new InfluxException(new SocketTimeoutException("Simulate error: SocketTimeoutException"))).when(mockWriteApi)
                .writeRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_RETRY);

        assertEquals("Simulate error: SocketTimeoutException", flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }
    
    @Test
    public void testWriteServiceUnavailableExceptionRetry() {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        Mockito.doThrow(new InfluxException(createErrorResponse(503))).when(mockWriteApi)
                .writeRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_RETRY);

        Assertions.assertThat(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE)).endsWith("Simulate error: 503");
    }

    @Test
    public void testWriteTooManyRequestsExceptionRetry() {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        Mockito.doThrow(new InfluxException(createErrorResponse(429))).when(mockWriteApi)
                .writeRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_RETRY);

        Assertions.assertThat(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE)).endsWith("Simulate error: 429");
    }

    @Test
    public void testWriteTooManyRequestsExceptionRetryAfterHeader() {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        Map<String, String> headers = Collections.singletonMap("Retry-After", "149");
        
        Mockito.doThrow(new InfluxException(createErrorResponse(429, headers))).when(mockWriteApi)
                .writeRecord(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_RETRY, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_RETRY);

        assertEquals("149", flowFiles.get(0).getAttribute(INFLUX_DB_RETRY_AFTER));
    }

    @Test
    public void testCannotInstantiateInfluxDBClient() throws InitializationException, IOException, GeneralSecurityException {

        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();
        
        PutInfluxDatabase_2 putInfluxDatabase_2 = new PutInfluxDatabase_2() {
            @Override
            public synchronized InfluxDBClient getInfluxDBClient(final ProcessContext context) {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "my-bucket");
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE),"testException");
    }

    @Test
    public void testWrittenRecord() {
        byte[] bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".getBytes();

        runner.enqueue(bytes);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        assertNull(flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
        Mockito.verify(mockWriteApi, Mockito.times(1))
                .writeRecord("my-bucket", "my-org", WritePrecision.NS, "h2o_feet,location=coyote_creek level\\ water_level=1.0 1");
    }
}