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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.influxdata.Cancellable;
import org.influxdata.client.InfluxDBClient;
import org.influxdata.client.QueryApi;
import org.influxdata.client.domain.Query;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;

import com.google.common.collect.Lists;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;

/**
 * @author Jakub Bednar (bednar@github) (22/07/2019 08:11)
 */
abstract class AbstractTestGetInfluxDatabase_2 {

    TestRunner runner;
    MockComponentLog logger;

    Answer queryAnswer = invocation -> Void.class;
    Exception queryOnErrorValue = null;
    List<String> queryOnResponseRecords = Lists.newArrayList();
    QueryApi mockQueryApi;

    GetInfluxDatabase_2 processor;

    @Before
    public void before() throws IOException, GeneralSecurityException, InitializationException {

        InfluxDBClient mockInfluxDBClient = Mockito.mock(InfluxDBClient.class);
        mockQueryApi = Mockito.mock(QueryApi.class);
        Mockito.doAnswer(invocation -> mockQueryApi).when(mockInfluxDBClient).getQueryApi();
        Mockito.doAnswer(invocation -> {
            if (queryOnErrorValue != null) {
                //noinspection unchecked
                Consumer<Exception> onError = invocation.getArgumentAt(3, Consumer.class);
                onError.accept(queryOnErrorValue);
            }

            queryOnResponseRecords.forEach(record -> {
                //noinspection unchecked
                BiConsumer<Cancellable, String> onRecord = invocation.getArgumentAt(2, BiConsumer.class);
                onRecord.accept(Mockito.mock(Cancellable.class), record);
            });

            boolean wasException = queryOnErrorValue != null;
            try {
                return queryAnswer.answer(invocation);
            } catch (Exception e){
                wasException = true;
                throw e;
            } finally {
                if (!wasException) {
                    Runnable onComplete = invocation.getArgumentAt(4, Runnable.class);
                    onComplete.run();
                }
            }
        }).when(mockQueryApi).queryRaw(Mockito.any(Query.class), Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any());

        processor = Mockito.spy(new GetInfluxDatabase_2());

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(GetInfluxDatabase_2.ORG, "my-org");
        runner.setProperty(GetInfluxDatabase_2.QUERY, "from(bucket:\"my-bucket\") |> range(start: 0) |> last()");
        runner.setProperty(GetInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());
        Mockito.doAnswer(invocation -> mockInfluxDBClient).when(influxDatabaseService).create();

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        logger = initContext.getLogger();
        processor.initialize(initContext);
        processor.onScheduled(runner.getProcessContext());
        processor.initWriterFactory(runner.getProcessContext());
    }
}