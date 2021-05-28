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

import com.influxdb.client.InfluxDBClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class TestStandardInfluxDatabaseServiceSettings_2 extends AbstractTestStandardInfluxDatabaseService_2 {

    @Before
    public void before() throws Exception {

        setUp(() -> invocation -> Mockito.mock(InfluxDBClient.class));
    }

    @Test
    public void defaultSettingsIsValid() {

        testRunner.assertValid(service);
    }

    @Test
    public void defaultSettings() throws IOException, GeneralSecurityException {

        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token"),
                Mockito.eq(null),
				Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void sslContextService() throws InitializationException, IOException, GeneralSecurityException {

        SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn("inluxdb-ssl");
        testRunner.addControllerService("inluxdb-ssl", sslContextService);
        testRunner.enableControllerService(sslContextService);

        testRunner.setProperty(service, InfluxDatabaseService_2.SSL_CONTEXT_SERVICE, "inluxdb-ssl");
        testRunner.assertValid(service);

        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token"),
                Mockito.eq(sslContextService),
				Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void clientAuth() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDatabaseService_2.CLIENT_AUTH, SSLContextService.ClientAuth.NONE.name());
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token"),
                Mockito.eq(null),
				Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void url() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_URL, "http://influxdb:8886");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token"),
                Mockito.eq(null),
				Mockito.eq("http://influxdb:8886"),
                Mockito.eq(0L));
    }

    @Test
    public void urlValidation() {

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_URL, "not_url");
        testRunner.assertNotValid(service);

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_URL, "");
        testRunner.assertNotValid(service);
    }

    @Test
    public void dbConnectionTimeout() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_CONNECTION_TIMEOUT, "100 mins");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token"),
                Mockito.eq(null),
				Mockito.eq("http://localhost:8086"),
                Mockito.eq(6000L));
    }

    @Test
    public void dbConnectionTimeoutValidation() {

        // not number value
        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_CONNECTION_TIMEOUT, "text");
        testRunner.assertNotValid(service);

        // without unit
        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_CONNECTION_TIMEOUT, "100");
        testRunner.assertNotValid(service);
    }

    @Test
    public void token() throws IOException, GeneralSecurityException {

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, "my-token-2");
        testRunner.assertValid(service);
        testRunner.enableControllerService(service);

        service.create();

        Mockito.verify(service, Mockito.times(1)).connect(
                Mockito.eq("my-token-2"),
                Mockito.eq(null),
				Mockito.eq("http://localhost:8086"),
                Mockito.eq(0L));
    }

    @Test
    public void tokenValidation() {

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, "");
        testRunner.assertNotValid(service);

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, " ");
        testRunner.assertNotValid(service);
    }
}
