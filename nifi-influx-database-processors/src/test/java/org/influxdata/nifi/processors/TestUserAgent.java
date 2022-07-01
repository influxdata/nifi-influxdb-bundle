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
import java.util.Date;

import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService;
import org.influxdata.nifi.services.StandardInfluxDatabaseService;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.influxdata.nifi.util.InfluxDBUtils;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestUserAgent {

    private MockWebServer server;
    private String baseURL;

    @Before
    public void setUp() {
        server = new MockWebServer();
        try {
            server.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        baseURL = server.url("/").url().toString();
    }

    @After
    public void after() throws IOException {
        server.shutdown();
    }

    @Test
    public void testPutInfluxDatabase() throws InterruptedException {

        server.enqueue(new MockResponse());
        server.enqueue(new MockResponse());

        TestRunner runner = TestRunners.newTestRunner(PutInfluxDatabase.class);
        runner.setProperty(ExecuteInfluxDatabaseQuery.DB_NAME, "my-db");
        runner.setProperty(ExecuteInfluxDatabaseQuery.USERNAME, "my-user");
        runner.setProperty(ExecuteInfluxDatabaseQuery.PASSWORD, "my-password");
        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_URL, baseURL);
        runner.setProperty(ExecuteInfluxDatabaseQuery.CHARSET, "UTF-8");
        runner.assertValid();

        String message = "water,country=US,city=newark rain=1,humidity=0.6 1";
        byte[] bytes = message.getBytes();
        runner.enqueue(bytes);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 1);

        Assert.assertEquals(1, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-java/");

        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_CLIENT_TYPE, "my-nifi");
        runner.assertValid();

        runner.enqueue(bytes);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutInfluxDatabase.REL_SUCCESS, 2);

        Assert.assertEquals(2, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-my-nifi/");
    }

    @Test
    public void testExecuteInfluxDatabaseQuery() throws InterruptedException {
        server.enqueue(new MockResponse().setBody("{}"));
        server.enqueue(new MockResponse().setBody("{}"));

        TestRunner runner = TestRunners.newTestRunner(ExecuteInfluxDatabaseQuery.class);
        runner.setProperty(ExecuteInfluxDatabaseQuery.DB_NAME, "my-db");
        runner.setProperty(ExecuteInfluxDatabaseQuery.USERNAME, "my-user");
        runner.setProperty(ExecuteInfluxDatabaseQuery.PASSWORD, "my-password");
        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_URL, baseURL);
        runner.setProperty(ExecuteInfluxDatabaseQuery.CHARSET, "UTF-8");
        runner.assertValid();

        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_QUERY, "select * from water");

        runner.setIncomingConnection(false);
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDatabaseQuery.REL_SUCCESS, 1);

        Assert.assertEquals(1, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-java/");

        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_CLIENT_TYPE, "my-nifi");
        runner.assertValid();

        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(ExecuteInfluxDatabaseQuery.REL_SUCCESS, 2);

        Assert.assertEquals(2, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-my-nifi/");
    }

    @Test
    public void testPutInfluxDatabaseRecord() throws InterruptedException, InitializationException {
        server.enqueue(new MockResponse().setBody("{}"));
        server.enqueue(new MockResponse().setBody("{}"));

        PutInfluxDatabaseRecord processor = new PutInfluxDatabaseRecord();

        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutInfluxDatabaseRecord.DB_NAME, "my-db");
        runner.setProperty(InfluxDBUtils.MEASUREMENT, "testRecordMeasurement");
        runner.setProperty(PutInfluxDatabaseRecord.RECORD_READER_FACTORY, "recordReader");
        runner.setProperty(PutInfluxDatabaseRecord.INFLUX_DB_SERVICE, "influxdb-service");

        MockRecordParser recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        InfluxDatabaseService influxDatabaseService = new StandardInfluxDatabaseService();
        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, InfluxDatabaseService.INFLUX_DB_URL, baseURL);
        runner.enableControllerService(influxDatabaseService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(initContext);

        runner.setProperty(InfluxDBUtils.TAGS, "lang");
        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "timestamp");
        runner.setProperty(InfluxDBUtils.FIELDS, "retweet_count");

        recordReader.addSchemaField("lang", RecordFieldType.STRING);
        recordReader.addSchemaField("retweet_count", RecordFieldType.INT);
        recordReader.addSchemaField("timestamp", RecordFieldType.LONG);

        recordReader.addRecord("en", 10, new Date());

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutInfluxDatabaseRecord.REL_SUCCESS, 1);

        Assert.assertEquals(1, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-java/");

        runner.disableControllerService(influxDatabaseService);
        runner.setProperty(influxDatabaseService, InfluxDatabaseService.INFLUX_DB_CLIENT_TYPE, "my-nifi");
        runner.enableControllerService(influxDatabaseService);

        recordReader.addRecord("cs", 25, new Date());

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutInfluxDatabaseRecord.REL_SUCCESS, 2);

        Assert.assertEquals(2, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-my-nifi/");
    }

    @Test
    public void testPutInfluxDatabase2() throws InterruptedException, InitializationException {
        server.enqueue(new MockResponse().setBody("{}"));
        server.enqueue(new MockResponse().setBody("{}"));

        PutInfluxDatabase_2 processor = new PutInfluxDatabase_2();

        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "my-bucket");
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        StandardInfluxDatabaseService_2 influxDatabaseService = new StandardInfluxDatabaseService_2();
        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, StandardInfluxDatabaseService_2.INFLUX_DB_URL, baseURL);
        runner.setProperty(influxDatabaseService, StandardInfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        Assert.assertEquals(1, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-java/");

        runner.disableControllerService(influxDatabaseService);
        runner.setProperty(influxDatabaseService, StandardInfluxDatabaseService_2.INFLUX_DB_CLIENT_TYPE, "my-nifi");
        runner.enableControllerService(influxDatabaseService);

        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 2);

        Assert.assertEquals(2, server.getRequestCount());
        Assertions.assertThat(server.takeRequest().getHeaders().get("User-Agent")).startsWith("influxdb-client-my-nifi/");
    }
}
