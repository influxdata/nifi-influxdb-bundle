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

import java.util.Arrays;
import java.util.Optional;

import org.influxdata.client.domain.Dialect;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * @author Jakub Bednar (bednar@github) (19/07/2019 10:23)
 */
public abstract class AbstractGetInfluxDatabase extends AbstractInfluxDatabaseProcessor_2 {

    public static final String INFLUXDB_ORG_NAME = "influxdb.org.name";

    public static final PropertyDescriptor ORG = new PropertyDescriptor.Builder()
            .name("influxdb-org")
            .displayName("Organization")
            .description("Specifies the source organization")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("influxdb-flux")
            .displayName("Query")
            .description("A valid Flux query to use to execute against InfluxDB")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIALECT_HEADER = new PropertyDescriptor.Builder()
            .name("influxdb-dialect-header")
            .displayName("Dialect Header")
            .description("If true, the results will contain a header row.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("false", "true")
            .defaultValue(Boolean.FALSE.toString())
            .required(true)
            .build();

    public static final PropertyDescriptor DIALECT_DELIMITER = new PropertyDescriptor.Builder()
            .name("influxdb-dialect-delimiter")
            .displayName("Dialect Delimiter")
            .description("Separator between cells; the default is ,")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIALECT_ANNOTATIONS = new PropertyDescriptor.Builder()
            .name("influxdb-dialect-annotations")
            .displayName("Dialect Annotations")
            .description("Describing properties about the columns of the table."
                    + " More than one can be supplied if comma separated. "
                    + "Allowable Values: group, datatype, default.")
            .required(false)
            .addValidator((subject, input, context) -> {


                if (input == null || input.isEmpty()) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
                }

                Optional<ValidationResult> invalidAnnotation = Arrays.stream(input.split(","))
                        .filter(annotation -> annotation != null && !annotation.trim().isEmpty())
                        .map(String::trim)
                        .filter((annotation) -> Dialect.AnnotationsEnum.fromValue(annotation) == null)
                        .map((annotation) -> new ValidationResult.Builder().subject(subject).input(input).explanation("Not a valid annotation").valid(false).build())
                        .findFirst();

                return invalidAnnotation.orElseGet(() -> new ValidationResult.Builder().subject(subject).input(input).explanation("Valid Annotation(s)").valid(true).build());
            })
            .build();

    public static final PropertyDescriptor DIALECT_COMMENT_PREFIX = new PropertyDescriptor.Builder()
            .name("influxdb-dialect-commentPrefix")
            .displayName("Dialect Comment Prefix")
            .description("Character prefixed to comment strings.")
            .required(true)
            .defaultValue("#")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIALECT_DATE_TIME_FORMAT = new PropertyDescriptor.Builder()
            .name("influxdb-dialect-dateTimeFormat")
            .displayName("Dialect Date Time Format")
            .description("Format of timestamps.")
            .required(true)
            .defaultValue("#")
            .defaultValue("RFC3339")
            .allowableValues("RFC3339", "RFC3339Nano")
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful Flux queries are routed to this relationship").build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed Flux queries are routed to this relationship").build();

    protected static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("Failed queries that are retryable exception are routed to this relationship").build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    }
}