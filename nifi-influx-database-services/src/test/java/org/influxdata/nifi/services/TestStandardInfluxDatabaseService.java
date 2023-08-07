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
package org.influxdata.nifi.services;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.mockito.stubbing.Answer;

public class TestStandardInfluxDatabaseService extends AbstractTestStandardInfluxDatabaseService {

    private Answer answerConnect;

    @BeforeEach
    public void before() throws Exception {

        setUp(() -> answerConnect);
    }

    @Test
    public void successConnect() throws IOException, GeneralSecurityException {

        answerConnect = new CallsRealMethods();

        testRunner.enableControllerService(service);

        InfluxDB influxDB = service.connect();

        Assertions.assertNotNull(influxDB);
    }

    @Test
    public void errorHandling() throws IOException, GeneralSecurityException {

        answerConnect = invocation -> {

            throw new InfluxDBIOException(new IOException("simulate exception"));
        };

        testRunner.enableControllerService(service);

        RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, () -> service.connect());
        Assertions.assertEquals("Error while getting connection java.io.IOException: simulate exception", runtimeException.getMessage());
    }
}
