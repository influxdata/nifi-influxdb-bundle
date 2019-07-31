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
import java.util.List;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.influxdata.nifi.util.InfluxDBUtils;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;

/**
 * @author Jakub Bednar (bednar@github) (17/07/2019 10:46)
 */
abstract class AbstractTestPutInfluxDatabaseRecord_2 {

    PutInfluxDatabaseRecord_2 processor;
    private InfluxDBClient mockInfluxDBClient;
    WriteApiBlocking mockWriteApi;
    protected Answer writeAnswer = invocation -> Void.class;

    @Captor
    protected ArgumentCaptor<List<Point>> pointCapture;

    TestRunner runner;
    protected MockRecordParser recordReader;
    protected MockComponentLog logger;

    @Before
    public void before() throws InitializationException, IOException, GeneralSecurityException {

        MockitoAnnotations.initMocks(this);

        mockWriteApi = Mockito.mock(WriteApiBlocking.class);
        mockInfluxDBClient = Mockito.mock(InfluxDBClient.class);
        Mockito.doAnswer(invocation -> mockWriteApi).when(mockInfluxDBClient).getWriteApiBlocking();

        processor = Mockito.spy(new PutInfluxDatabaseRecord_2());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutInfluxDatabaseRecord_2.ORG, "my-org");
        runner.setProperty(PutInfluxDatabaseRecord_2.BUCKET, "my-bucket");
        runner.setProperty(InfluxDBUtils.MEASUREMENT, "nifi-measurement");
        runner.setProperty(InfluxDBUtils.FIELDS, "nifi-field");
        runner.setProperty(AbstractInfluxDatabaseProcessor.RECORD_READER_FACTORY, "recordReader");
        runner.setProperty(PutInfluxDatabaseRecord_2.INFLUX_DB_SERVICE, "influxdb-service");

        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();
        Mockito.doAnswer(invocation -> {

            Mockito.doAnswer(writeAnswer).when(mockWriteApi).writePoints(Mockito.any(), Mockito.any(), pointCapture.capture());

            return mockInfluxDBClient;

        }).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        logger = initContext.getLogger();
        processor.initialize(initContext);

        /**
         * Manually call onScheduled because it si not called by
         * org.apache.nifi.util.StandardProcessorTestRunner#run(int, boolean, boolean, long).
         * Bug in Mockito-CGLIB (https://github.com/mockito/mockito/issues/204)... remove after upgrade Mockito to 2.x
         */
        processor.onScheduled(runner.getProcessContext());
    }

    @After
    public void after() {

        processor.close();
    }
}