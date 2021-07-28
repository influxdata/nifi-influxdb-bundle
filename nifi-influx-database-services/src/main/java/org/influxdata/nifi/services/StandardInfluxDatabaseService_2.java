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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import okhttp3.OkHttpClient;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;

import static org.influxdata.nifi.util.PropertyValueUtils.getEnumValue;

@Tags({"influxdb", "client", "2.0"})
@CapabilityDescription("The controller service that provides connection to InfluxDB 2.0.")
public class StandardInfluxDatabaseService_2 extends AbstractInfluxDatabaseService implements InfluxDatabaseService_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors.add(INFLUX_DB_URL);
        propertyDescriptors.add(INFLUX_DB_CONNECTION_TIMEOUT);
        propertyDescriptors.add(INFLUX_DB_ACCESS_TOKEN);

        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        return PROPERTY_DESCRIPTORS;
    }

    @NonNull
    @Override
    public InfluxDBClient create() {

        ConfigurationContext context = getConfigurationContext();

        // SSL
        SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        ClientAuth clientAuth = getEnumValue(CLIENT_AUTH, context, ClientAuth.class, DEFAULT_CLIENT_AUTH);

        // Connection
        String influxDbUrl = getDatabaseURL();
        long connectionTimeout = context.getProperty(INFLUX_DB_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS);

        // Credentials
        String token = context.getProperty(INFLUX_DB_ACCESS_TOKEN).evaluateAttributeExpressions().getValue();

        try {
            InfluxDBClient client = connect(token, sslService, clientAuth, influxDbUrl, connectionTimeout);

            getLogger().info("InfluxDB 2.0 connection created for host {}", new Object[]{influxDbUrl});

            return client;

        } catch (Exception e) {

            getLogger().error("Error while getting connection {}", new Object[]{e.getLocalizedMessage()}, e);

            throw new RuntimeException("Error while getting connection " + e.getLocalizedMessage(), e);
        }
    }

    @NonNull
    @Override
    public String getDatabaseURL() {

        ConfigurationContext context = getConfigurationContext();

        return context.getProperty(INFLUX_DB_URL).evaluateAttributeExpressions().getValue();
    }

    @NonNull
    protected InfluxDBClient connect(final String token,
                                     final SSLContextService sslService,
                                     final ClientAuth clientAuth,
                                     final String influxDbUrl,
                                     final long connectionTimeout) throws IOException {


        OkHttpClient.Builder builder = new OkHttpClient.Builder().connectTimeout(connectionTimeout, TimeUnit.SECONDS);
        if (sslService != null) {
            configureSSL(builder, clientAuth, sslService);
        }

        return InfluxDBClientFactory.create(influxDbUrl, token.toCharArray());
    }

}

