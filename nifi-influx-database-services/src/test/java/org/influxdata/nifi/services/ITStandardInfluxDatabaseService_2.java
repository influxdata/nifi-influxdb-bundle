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

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

public class ITStandardInfluxDatabaseService_2 extends AbstractTestStandardInfluxDatabaseService_2 {

    @Before
    public void before() throws Exception {

        setUp(CallsRealMethods::new);

        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_URL, "http://localhost:9999");
        testRunner.setProperty(service, InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN, "my-token");
    }

    @Test
    public void connectionWithDefaultSettings() throws IOException, GeneralSecurityException {

        testRunner.enableControllerService(service);

        assertConnectToDatabase();
    }
}
