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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.exceptions.InfluxException;
import org.influxdata.nifi.services.InfluxDatabaseService_2;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import retrofit2.Response;

import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.util.PropertyValueUtils.getEnumValue;

/**
 * Abstract base class for InfluxDB 2.0 processors.
 *
 * @author Jakub Bednar (bednar@github) (11/07/2019 07:47)
 */
public abstract class AbstractInfluxDatabaseProcessor_2 extends AbstractProcessor {

    public static final String ORG_NAME_EMPTY_MESSAGE =
            "Cannot write FlowFile to InfluxDB because destination organization is null or empty.";

    public static final String BUCKET_NAME_EMPTY_MESSAGE =
            "Cannot write FlowFile to InfluxDB because destination bucket is null or empty.";

    /**
     * Influx Log levels.
     */
    private static final AllowableValue NONE =
            new AllowableValue("NONE", "None", "No logging");

    private static final AllowableValue BASIC =
            new AllowableValue("BASIC", "Basic",
                    "Log only the request method and URL and the response status code and execution time.");

    private static final AllowableValue HEADERS =
            new AllowableValue("HEADERS", "Headers",
                    "Log the basic information along with request and response headers.");

    private static final AllowableValue BODY =
            new AllowableValue("BODY", "Body",
                    "Log the headers, body, and metadata for both requests and responses. "
                            + "Note: This requires that the entire request and response body be buffered in memory!");

    public static final PropertyDescriptor INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("influxdb-service")
            .displayName("InfluxDB Controller Service")
            .description("A controller service that provides connection to InfluxDB")
            .required(true)
            .identifiesControllerService(InfluxDatabaseService_2.class)
            .build();

    public static final PropertyDescriptor BUCKET = new PropertyDescriptor.Builder()
            .name("influxdb-bucket")
            .displayName("Bucket")
            .description("Specifies the destination bucket for writes")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ORG = new PropertyDescriptor.Builder()
            .name("influxdb-org")
            .displayName("Organization")
            .description("Specifies the destination organization for writes")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMESTAMP_PRECISION = new PropertyDescriptor.Builder()
            .name("influxdb-write-precision")
            .displayName("Timestamp precision")
            .description("The precision of the time stamps.")
            .required(true)
            .defaultValue(WritePrecision.NS.name())
            .allowableValues(Arrays.stream(WritePrecision.values()).map(Enum::name).toArray(String[]::new))
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final PropertyDescriptor ENABLE_GZIP = new PropertyDescriptor.Builder()
            .name("influxdb-enable-gzip")
            .displayName("Enable gzip compression")
            .description("Enable gzip compression for InfluxDB http request body.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("false", "true")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("influxdb-log-level")
            .displayName("Log Level")
            .description("Controls the level of logging for the REST layer of InfluxDB client.")
            .required(true)
            .allowableValues(NONE, BASIC, HEADERS, BODY)
            .defaultValue(NONE.getValue())
            .build();

    private AtomicReference<InfluxDBClient> influxDBClient = new AtomicReference<>();

    protected InfluxDatabaseService_2 influxDatabaseService;
    protected long maxRecordsSize;

    /**
     * Assigns the InfluxDB 2.0 Service on scheduling.
     *
     * @param context the process context provided on scheduling the processor.
     */
    @OnScheduled
    public void onScheduled(@NonNull final ProcessContext context) {

        Objects.requireNonNull(context, "ProcessContext is required");

        influxDatabaseService = context.getProperty(INFLUX_DB_SERVICE).asControllerService(InfluxDatabaseService_2.class);

        if (getSupportedPropertyDescriptors().contains(MAX_RECORDS_SIZE)) {
            maxRecordsSize = context.getProperty(MAX_RECORDS_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue();
        }
    }

    @OnStopped
    public void close() {
        if (getLogger().isDebugEnabled()) {
            getLogger().info("Closing connection");
        }
        if (influxDBClient.get() != null) {
            try {
                influxDBClient.get().close();
            } catch (Exception e) {
                getLogger().error("The InfluxDBClient throws exception during closing", e);
            } finally {
                influxDBClient.set(null);
            }
        }
    }

    /**
     * Get or create InfluxDBClient thought {@link InfluxDatabaseService_2}.
     *
     * @return InfluxDBClient instance
     */
    public synchronized InfluxDBClient getInfluxDBClient(final ProcessContext context) {

        if (influxDBClient.get() == null) {

            try {
                InfluxDBClient influxDBClient = influxDatabaseService.create();
                configure(influxDBClient, context);

                this.influxDBClient.set(influxDBClient);

            } catch (Exception e) {

                String message = "Error while getting connection " + e.getLocalizedMessage();

                getLogger().error(message, e);

                throw new RuntimeException(message, e);
            }

            getLogger().info("InfluxDB connection created for host {}", new Object[]{influxDatabaseService.getDatabaseURL()});
        }

        return influxDBClient.get();
    }

	@Nullable
	protected String getRetryAfterHeader(InfluxException ie) {
		try
		{
            //
			// Temporally solution before release https://github.com/influxdata/influxdb-client-java/pull/317
            //
			Field responseField = InfluxException.class.getDeclaredField("response");
			responseField.setAccessible(true);
			Response response = (Response) responseField.get(ie);
			if (response != null) {
				return response.headers().get("Retry-After");
			}
		}
		catch (Exception e)
		{
			return null;
		}

		return null;
	}

	/**
     * Configure LogLevel and GZIP.
     */
    private void configure(@NonNull final InfluxDBClient influxDBClient, @NonNull final ProcessContext context) {

        Objects.requireNonNull(influxDBClient, "InfluxDBClient instance is required for configuration");
        Objects.requireNonNull(context, "Context of Processor is required");

        // GZIP
        Boolean enableGzip = context.getProperty(ENABLE_GZIP).asBoolean();
        if (BooleanUtils.isTrue(enableGzip)) {

            influxDBClient.enableGzip();
        } else {

            influxDBClient.disableGzip();
        }

        // LOG Level
        LogLevel logLevel = getEnumValue(LOG_LEVEL, context, LogLevel.class, LogLevel.NONE);
        influxDBClient.setLogLevel(logLevel);
    }
}