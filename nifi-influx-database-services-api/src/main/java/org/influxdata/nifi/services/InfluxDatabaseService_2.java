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

import org.influxdata.client.InfluxDBClient;
import org.influxdata.client.InfluxDBClientFactory;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/**
 * This controller service interface providing client connection to InfluxDB 2.0.
 */
public interface InfluxDatabaseService_2 extends ControllerService {

    SSLContextService.ClientAuth DEFAULT_CLIENT_AUTH = SSLContextService.ClientAuth.REQUIRED;

    PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("inluxdb-ssl")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("inluxdb-ssl-auth")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. "
                    + "Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue(DEFAULT_CLIENT_AUTH.name())
            .build();

    PropertyDescriptor INFLUX_DB_URL = new PropertyDescriptor.Builder()
            .name("influxdb-url")
            .displayName("InfluxDB connection URL")
            .description("The URL to connect to. Eg: http://influxdb:8086")
            .defaultValue("http://localhost:8086")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    PropertyDescriptor INFLUX_DB_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("influxdb-connection-timeout")
            .displayName("InfluxDB Max Connection Time Out (seconds)")
            .description("The maximum time for establishing connection to the InfluxDB.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    PropertyDescriptor INFLUX_DB_ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("influxdb-token")
            .displayName("InfluxDB Access Token")
            .description("Access Token used for authenticating/authorizing the InfluxDB request sent by NiFi.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    /**
     * Create a connection to a InfluxDB 2.0.
     *
     * @return an adapter suitable to access a InfluxDB 2.0.
     * @see InfluxDBClientFactory#create(String)
     */
    @NonNull
    InfluxDBClient create() throws IOException, GeneralSecurityException;

    /**
     * @return a InfluxDB url
     */
    @NonNull
    String getDatabaseURL();
}
