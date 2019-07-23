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

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.influxdata.Cancellable;
import org.influxdata.client.domain.Dialect;
import org.influxdata.client.domain.Query;
import org.influxdata.exceptions.InfluxException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_FAIL_TO_QUERY;

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

    public static final PropertyDescriptor RECORDS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("influxdb-records-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many records to put into a FlowFile at once. The whole body will be treated as a CSV file.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful Flux queries are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed Flux queries are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("Failed queries that are retryable exception are routed to this relationship").build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        boolean createdFlowFile = false;
        FlowFile flowFile = session.get();

        // If there aren't incoming connections.
        if (flowFile == null) {
            flowFile = session.create();
            createdFlowFile = true;
        }

        String org = context.getProperty(ORG).evaluateAttributeExpressions(flowFile).getValue();
        String flux = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();

        boolean dialectHeader = context.getProperty(DIALECT_HEADER).asBoolean();
        String dialectDelimiter = context.getProperty(DIALECT_DELIMITER).getValue();
        String dialectCommentPrefix = context.getProperty(DIALECT_COMMENT_PREFIX).getValue();
        Dialect.DateTimeFormatEnum dialectTimeFormat = Dialect.DateTimeFormatEnum
                .fromValue(context.getProperty(DIALECT_DATE_TIME_FORMAT).getValue());

        List<Dialect.AnnotationsEnum> dialectAnnotations = new ArrayList<>();
        String dialectAnnotationsValue = context.getProperty(DIALECT_ANNOTATIONS).getValue();
        if (dialectAnnotationsValue != null && !dialectAnnotationsValue.isEmpty()) {
            Arrays.stream(dialectAnnotationsValue.split(","))
                    .filter(annotation -> annotation != null && !annotation.trim().isEmpty())
                    .map(String::trim)
                    .forEach(annotation -> dialectAnnotations.add(Dialect.AnnotationsEnum.fromValue(annotation.trim())));
        }


        long sizePerBatch = -1;
        if (context.getProperty(RECORDS_PER_FLOWFILE).isSet()) {
            sizePerBatch = context.getProperty(RECORDS_PER_FLOWFILE).evaluateAttributeExpressions().asLong();
        }


        try {

            Dialect dialect = new Dialect()
                    .header(dialectHeader)
                    .delimiter(dialectDelimiter)
                    .commentPrefix(dialectCommentPrefix)
                    .dateTimeFormat(dialectTimeFormat)
                    .annotations(dialectAnnotations);

            Query query = new Query()
                    .query(flux)
                    .dialect(dialect);

            new QueryProcessor(org, query, sizePerBatch, flowFile, createdFlowFile, context, session).doQueryRaw();

        } catch (Exception e) {
            catchException(e, Collections.singletonList(flowFile), context, session);
        }
    }

    private void catchException(final Throwable e,
                                final List<FlowFile> flowFiles,
                                final ProcessContext context,
                                final ProcessSession session) {

        String message = INFLUX_DB_FAIL_TO_QUERY;
        Object status = e.getClass().getSimpleName();
        Relationship relationship = REL_FAILURE;

        if (e instanceof InfluxException) {

            InfluxException ie = (InfluxException) e;
            status = ie.status();

            // Retryable
            if (Arrays.asList(429, 503).contains(ie.status()) || ie.getCause() instanceof SocketTimeoutException) {
                message += " ... retry";
                relationship = REL_RETRY;
            }
        }

        for (FlowFile flowFile : flowFiles) {

            if (REL_RETRY.equals(relationship)) {
                session.penalize(flowFile);
            }

            session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, relationship);
        }

        getLogger().error(message, new Object[]{status, e.getLocalizedMessage()}, e);
        context.yield();
    }

    private class QueryProcessor {
        private FlowFile flowFile;
        private final List<FlowFile> flowFiles = new ArrayList<>();
        private final String org;
        private final long sizePerBatch;
        private final Query query;
        private final ProcessContext context;
        private final ProcessSession session;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private final StopWatch stopWatch = new StopWatch();

        private long recordIndex = 0;

        private QueryProcessor(final String org,
                               final Query query,
                               final long sizePerBatch,
                               final FlowFile flowFile,
                               final boolean createdFlowFile, final ProcessContext context,
                               final ProcessSession session) {
            this.flowFiles.add(flowFile);
            if (sizePerBatch == -1 || createdFlowFile) {
                this.flowFile = flowFile;
            } else {
                this.flowFile = session.create();
                this.flowFiles.add(this.flowFile);
            }
            this.org = org;
            this.sizePerBatch = sizePerBatch;
            this.query = query;
            this.context = context;
            this.session = session;

            this.stopWatch.start();
        }

        void doQueryRaw() {
            try {

                getInfluxDBClient(context)
                        .getQueryApi()
                        .queryRaw(query, org, this::onResponse, this::onError, this::onComplete);

                countDownLatch.await();
            } catch (Exception e) {
                catchException(e, flowFiles, context, session);
            }
        }

        private void onResponse(Cancellable cancellable, String record) {

            if (record == null || record.isEmpty()) {
                return;
            }

            recordIndex++;
            if (sizePerBatch != -1 && recordIndex > sizePerBatch) {
                flowFile = session.create();
                flowFiles.add(flowFile);
                recordIndex = 1;
            }

            session.append(flowFile, out -> {
                if (recordIndex > 1) {
                    out.write('\n');
                }
                out.write(record.getBytes());
            });
        }

        private void onError(Throwable throwable) {
            stopWatch.stop();

            catchException(throwable, flowFiles, context, session);

            countDownLatch.countDown();
        }

        private void onComplete() {
            stopWatch.stop();

            session.transfer(flowFiles, REL_SUCCESS);
            
            for (FlowFile flowFile : flowFiles) {
                session.getProvenanceReporter()
                        .send(flowFile, influxDatabaseService.getDatabaseURL(), stopWatch.getElapsed(MILLISECONDS));
            }

            getLogger().debug("Query {} fetched in {}", new Object[]{query, stopWatch.getDuration()});

            countDownLatch.countDown();
        }
    }
}