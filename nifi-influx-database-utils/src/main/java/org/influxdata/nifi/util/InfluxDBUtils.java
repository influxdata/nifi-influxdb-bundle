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
package org.influxdata.nifi.util;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * @author Jakub Bednar (bednar@github) (29/05/2019 09:13)
 */
public final class InfluxDBUtils {

    public static final String MEASUREMENT_NAME_EMPTY_MESSAGE =
            "Cannot write FlowFile to InfluxDB because Measurement Name is null or empty.";

    public static final String AT_LEAST_ONE_FIELD_DEFINED_MESSAGE =
            "Cannot write FlowFile to InfluxDB because at least one field must be defined.";

    public static final String REQUIRED_FIELD_MISSING =
            "Cannot write FlowFile to InfluxDB because the required field '%s' is not present in Record.";

    public static final String UNSUPPORTED_FIELD_TYPE =
            "Cannot write FlowFile to InfluxDB because the field '%s' has a unsupported type '%s'.";

    public static final String FIELD_NULL_VALUE =
            "Cannot write FlowFile to InfluxDB because the field '%s' has null value.";

    public static final TimeUnit PRECISION_DEFAULT = TimeUnit.NANOSECONDS;
    public static final MissingItemsBehaviour MISSING_FIELDS_BEHAVIOUR_DEFAULT = MissingItemsBehaviour.IGNORE;
    public static final MissingItemsBehaviour MISSING_TAGS_BEHAVIOUR_DEFAULT = MissingItemsBehaviour.IGNORE;
    public static final ComplexFieldBehaviour COMPLEX_FIELD_BEHAVIOUR_DEFAULT = ComplexFieldBehaviour.TEXT;
    public static final NullValueBehaviour NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT = NullValueBehaviour.IGNORE;
    public static final String DEFAULT_RETENTION_POLICY = "autogen";

    /**
     * @see #MISSING_FIELD_BEHAVIOR
     * @see #MISSING_TAG_BEHAVIOR
     */
    public enum MissingItemsBehaviour {

        /**
         * @see #MISSING_ITEMS_BEHAVIOUR_IGNORE
         */
        IGNORE,

        /**
         * @see #MISSING_ITEMS_BEHAVIOUR_FAIL
         */
        FAIL,
    }

    public enum ComplexFieldBehaviour {

        /**
         * @see #COMPLEX_FIELD_IGNORE
         */
        IGNORE,

        /**
         * @see #COMPLEX_FIELD_FAIL
         */
        FAIL,

        /**
         * @see #COMPLEX_FIELD_WARN
         */
        WARN,

        /**
         * @see #COMPLEX_FIELD_VALUE
         */
        TEXT,
    }

    public enum NullValueBehaviour {
        /**
         * @see #NULL_VALUE_BEHAVIOUR_IGNORE
         */
        IGNORE,

        /**
         * @see #NULL_VALUE_BEHAVIOUR_FAIL
         */
        FAIL
    }

    /**
     * Missing items behaviour.
     */
    private static final AllowableValue MISSING_ITEMS_BEHAVIOUR_IGNORE = new AllowableValue(
            MissingItemsBehaviour.IGNORE.name(),
            "Ignore",
            "The item that is not present in the document is silently ignored.");

    private static final AllowableValue MISSING_ITEMS_BEHAVIOUR_FAIL = new AllowableValue(
            MissingItemsBehaviour.FAIL.name(),
            "Fail",
            "If the item is not present in the document, the FlowFile will be routed to the failure relationship.");


    /**
     * Null values behaviour.
     */
    private static final AllowableValue NULL_VALUE_BEHAVIOUR_IGNORE = new AllowableValue(
            NullValueBehaviour.IGNORE.name(),
            "Ignore",
            "Silently skip fields with a null value.");

    private static final AllowableValue NULL_VALUE_BEHAVIOUR_FAIL = new AllowableValue(
            NullValueBehaviour.FAIL.name(),
            "Fail",
            "Fail when the field has a null value.");

    /**
     * Complex field behaviour
     */
    protected static final AllowableValue COMPLEX_FIELD_FAIL = new AllowableValue(
            ComplexFieldBehaviour.FAIL.name(),
            "Fail",
            "Route entire FlowFile to failure if any elements contain complex values.");

    protected static final AllowableValue COMPLEX_FIELD_WARN = new AllowableValue(
            ComplexFieldBehaviour.WARN.name(),
            "Warn",
            "Provide a warning and do not include field InfluxDB data point.");

    protected static final AllowableValue COMPLEX_FIELD_IGNORE = new AllowableValue(
            ComplexFieldBehaviour.IGNORE.name(),
            "Ignore",
            "Silently ignore and do not include field InfluxDB data point.");

    protected static final AllowableValue COMPLEX_FIELD_VALUE = new AllowableValue(
            ComplexFieldBehaviour.TEXT.name(),
            "Text",
            "Use the string representation of the complex field as the value of the given field.");

    public static final PropertyDescriptor MEASUREMENT = new PropertyDescriptor.Builder()
            .name("influxdb-measurement")
            .displayName("Measurement")
            .description("The name of the measurement."
                    + " If the Record contains a field with measurement property value, "
                    + "then value of the Record field is use as InfluxDB measurement.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("measurement")
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("influxdb-fields")
            .displayName("Fields")
            .description("A comma-separated list of record fields stored in InfluxDB as 'field'. "
                    + " At least one field must be defined")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("fields")
            .build();

    public static final PropertyDescriptor TAGS = new PropertyDescriptor.Builder()
            .name("influxdb-tags")
            .displayName("Tags")
            .description("A comma-separated list of record fields stored in InfluxDB as 'tag'.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("tags")
            .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("influxdb-timestamp-field")
            .displayName("Timestamp field")
            .description("A name of the record field that used as a 'timestamp'. "
                    + "If it is not specified, current system time is used. "
                    + "The support types of field value are: java.util.Date, java.lang.Number, "
                    + "java.lang.String (parsable to Long).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("timestamp")
            .build();

    public static final PropertyDescriptor TIMESTAMP_PRECISION = new PropertyDescriptor.Builder()
            .name("influxdb-timestamp-precision")
            .displayName("Timestamp precision")
            .description("The timestamp precision is ignore when the 'Timestamp field' value is 'java.util.Date'.")
            .defaultValue(PRECISION_DEFAULT.name())
            .required(true)
            .allowableValues(Arrays.stream(TimeUnit.values()).map(Enum::name).toArray(String[]::new))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MISSING_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-fields-field-behavior")
            .displayName("Missing Field Behavior")
            .description("If the specified field is not present in the document, "
                    + "this property specifies how to handle the situation.")
            .allowableValues(MISSING_ITEMS_BEHAVIOUR_IGNORE, MISSING_ITEMS_BEHAVIOUR_FAIL)
            .defaultValue(MISSING_FIELDS_BEHAVIOUR_DEFAULT.name())
            .build();

    public static final PropertyDescriptor MISSING_TAG_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-tags-field-behavior")
            .displayName("Missing Tag Behavior")
            .description("If the specified tag is not present in the document, "
                    + "this property specifies how to handle the situation.")
            .allowableValues(MISSING_ITEMS_BEHAVIOUR_IGNORE, MISSING_ITEMS_BEHAVIOUR_FAIL)
            .defaultValue(MISSING_TAGS_BEHAVIOUR_DEFAULT.name())
            .build();

    public static final PropertyDescriptor COMPLEX_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-complex-field-behavior")
            .displayName("Complex Field Behavior")
            .description("Indicates how to handle complex fields, i.e. fields that do not have a primitive value.")
            .required(true)
            .allowableValues(COMPLEX_FIELD_VALUE, COMPLEX_FIELD_IGNORE, COMPLEX_FIELD_WARN, COMPLEX_FIELD_FAIL)
            .defaultValue(COMPLEX_FIELD_BEHAVIOUR_DEFAULT.name())
            .build();

    public static final PropertyDescriptor NULL_VALUE_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-null-behavior")
            .displayName("Null Values Behavior")
            .description("Indicates how to handle null fields, i.e. fields that do not have a defined value.")
            .required(true)
            .allowableValues(NULL_VALUE_BEHAVIOUR_IGNORE, NULL_VALUE_BEHAVIOUR_FAIL)
            .defaultValue(NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT.name())
            .build();

    /**
     * Create a connection to a InfluxDB.
     *
     * @param influxDbUrl       the url to connect to
     * @param username          the username which is used to authorize against the influxDB instance
     * @param password          the password for the username which is used to authorize against the influxDB instance
     * @param connectionTimeout the default connect timeout
     * @param configurer        to configure OkHttpClient.Builder with SSL
     * @param clientType        to customize the User-Agent HTTP header
     * @return InfluxDB client
     */
    @Nonnull
    public static InfluxDB makeConnectionV1(String influxDbUrl,
                                            String username,
                                            String password,
                                            long connectionTimeout,
                                            Consumer<OkHttpClient.Builder> configurer,
                                            final String clientType) {

        // get version of influxdb-java
        Package mainPackage = InfluxDBFactory.class.getPackage();
        String version = mainPackage != null ? mainPackage.getImplementationVersion() : null;

        // create User-Agent header content
        String userAgent = String.format("influxdb-client-%s/%s",
                clientType != null ? clientType : "java",
                version != null ? version : "unknown");

        OkHttpClient.Builder builder = new OkHttpClient
                .Builder()
                .connectTimeout(connectionTimeout, TimeUnit.SECONDS)
                .readTimeout(connectionTimeout, TimeUnit.SECONDS)
                .writeTimeout(connectionTimeout, TimeUnit.SECONDS)
                // add interceptor with "User-Agent" header
                .addInterceptor(chain -> {
                    Request request = chain
                            .request()
                            .newBuilder()
                            .header("User-Agent", userAgent)
                            .build();

                    return chain.proceed(request);
                });
        if (configurer != null) {
            configurer.accept(builder);
        }
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            return InfluxDBFactory.connect(influxDbUrl, builder);
        } else {
            return InfluxDBFactory.connect(influxDbUrl, username, password, builder);
        }
    }

    /**
     * Create a connection to a InfluxDB.
     *
     * @param influxDbUrl       the url to connect to
     * @param token             the token to use for the authorization
     * @param connectionTimeout the default connect timeout
     * @param configurer        to configure OkHttpClient.Builder with SSL
     * @param clientType        to customize the User-Agent HTTP header
     * @return InfluxDB client
     */
    @Nonnull
    public static InfluxDBClient makeConnectionV2(String influxDbUrl,
                                                  String token,
                                                  long connectionTimeout,
                                                  Consumer<OkHttpClient.Builder> configurer,
                                                  final String clientType) {
        OkHttpClient.Builder builder = new OkHttpClient
                .Builder()
                .connectTimeout(connectionTimeout, TimeUnit.SECONDS)
                .readTimeout(connectionTimeout, TimeUnit.SECONDS)
                .writeTimeout(connectionTimeout, TimeUnit.SECONDS);
        if (configurer != null) {
            configurer.accept(builder);
        }

        InfluxDBClientOptions.Builder options = InfluxDBClientOptions
                .builder()
                .url(influxDbUrl)
                .authenticateToken(token.toCharArray())
                .okHttpClient(builder);

        if (StringUtils.isNoneBlank(clientType)) {
            options.clientType(clientType);
        }

        return InfluxDBClientFactory.create(options.build());
    }
}