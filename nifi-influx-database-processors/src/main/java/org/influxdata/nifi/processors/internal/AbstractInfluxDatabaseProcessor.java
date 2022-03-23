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
package org.influxdata.nifi.processors.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

/**
 * Abstract base class for InfluxDB processors
 */
public abstract class AbstractInfluxDatabaseProcessor extends AbstractProcessor {

    public static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data "
                    + "and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("influxdb-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUX_DB_URL = new PropertyDescriptor.Builder()
            .name("influxdb-url")
            .displayName("InfluxDB connection URL")
            .description("InfluxDB URL to connect to. Eg: http://influxdb:8086")
            .defaultValue("http://localhost:8086")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor INFLUX_DB_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("InfluxDB Max Connection Time Out (seconds)")
            .description("The maximum time for establishing connection to the InfluxDB")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("influxdb-dbname")
            .displayName("Database Name")
            .description("InfluxDB database to connect to")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("influxdb-username")
            .displayName("Username")
            .required(false)
            .description("Username for accessing InfluxDB")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("influxdb-password")
            .displayName("Password")
            .required(false)
            .description("Password for user")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor MAX_RECORDS_SIZE = new PropertyDescriptor.Builder()
            .name("influxdb-max-records-size")
            .displayName("Max size of records")
            .description("Maximum size of records allowed to be posted in one batch")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1 MB")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful FlowFiles that are saved to InfluxDB are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles were not saved to InfluxDB are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("FlowFiles were not saved to InfluxDB due to retryable exception are routed to this relationship").build();

    public static final Relationship REL_MAX_SIZE_EXCEEDED = new Relationship.Builder().name("failure-max-size")
            .description("FlowFiles exceeding max records size are routed to this relationship").build();


    public static final String INFLUX_DB_ERROR_MESSAGE = "influxdb.error.message";
    public static final String INFLUX_DB_RETRY_AFTER = "influxdb.retry-after";
    public static final String INFLUX_DB_ERROR_MESSAGE_LOG = "Failed procession flow file {} due to {}";
    public static final String INFLUX_DB_FAIL_TO_INSERT = "Failed to insert into influxDB due to {}";
    public static final String INFLUX_DB_FAIL_TO_QUERY = "Failed to execute Flux query due {} to {}";

    protected AtomicReference<InfluxDB> influxDB = new AtomicReference<>();
    protected long maxRecordsSize;

    /**
     * Helper method to create InfluxDB instance
     * @return InfluxDB instance
     */
    protected synchronized InfluxDB getInfluxDB(ProcessContext context) {
        if ( influxDB.get() == null ) {
            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            long connectionTimeout = context.getProperty(INFLUX_DB_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS);
            String influxDbUrl = context.getProperty(INFLUX_DB_URL).evaluateAttributeExpressions().getValue();
            try {
                influxDB.set(makeConnection(username, password, influxDbUrl, connectionTimeout));
            } catch(Exception e) {
                getLogger().error("Error while getting connection {}", new Object[] { e.getLocalizedMessage() },e);
                throw new RuntimeException("Error while getting connection " + e.getLocalizedMessage(),e);
            }
            getLogger().info("InfluxDB connection created for host {}",
                    new Object[] {influxDbUrl});
        }
        return influxDB.get();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    protected InfluxDB makeConnection(String username, String password, String influxDbUrl, long connectionTimeout) {
        Builder builder = new OkHttpClient.Builder().connectTimeout(connectionTimeout, TimeUnit.SECONDS);
        if ( StringUtils.isBlank(username) || StringUtils.isBlank(password) ) {
            return InfluxDBFactory.connect(influxDbUrl, builder);
        } else {
            return InfluxDBFactory.connect(influxDbUrl, username, password, builder);
        }
    }

    @OnStopped
    public void close() {
        if (getLogger().isDebugEnabled()) {
            getLogger().info("Closing connection");
        }
        if ( influxDB.get() != null ) {
            influxDB.get().close();
            influxDB.set(null);
        }
    }
}