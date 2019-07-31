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

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import com.influxdb.Cancellable;
import com.influxdb.client.domain.Dialect;
import com.influxdb.client.domain.Query;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_FAIL_TO_QUERY;

/**
 * @author Jakub Bednar (bednar@github) (19/07/2019 10:23)
 */
public abstract class AbstractGetInfluxDatabase_2 extends AbstractInfluxDatabaseProcessor_2 {

    public static final PropertyDescriptor WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("influxdb-record-writer-factory")
            .displayName("Record Writer")
            .description("The record writer to use to write the result sets.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor ORG = new PropertyDescriptor.Builder()
            .name("influxdb-org")
            .displayName("Organization")
            .description("Specifies the source organization.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("influxdb-flux")
            .displayName("Query")
            .description("A valid Flux query to use to execute against InfluxDB.")
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

    public static PropertyDescriptor RECORDS_PER_FLOWFILE;

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successful Flux queries are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed Flux queries are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("Failed queries that are retryable exception are routed to this relationship").build();

    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void initWriterFactory(@NonNull final ProcessContext context) {

        Objects.requireNonNull(context, "ProcessContext is required");

        if (getSupportedPropertyDescriptors().contains(WRITER_FACTORY)) {
            writerFactory = context.getProperty(WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        }
    }

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
        Dialect dialect = prepareDialect(context);

        //
        // Records per Flowfile
        //
        long recordsPerFlowFile = -1;
        if (context.getProperty(RECORDS_PER_FLOWFILE).isSet()) {
            recordsPerFlowFile = context.getProperty(RECORDS_PER_FLOWFILE).evaluateAttributeExpressions().asLong();
        }

        Query query = new Query()
                .query(flux)
                .dialect(dialect);
        try {

            QueryProcessor processor = new QueryProcessor(org, query, recordsPerFlowFile, flowFile, createdFlowFile, context, session);

            // CVS or Record based response?
            if (getSupportedPropertyDescriptors().contains(WRITER_FACTORY)) {
                processor.doQuery();
            } else {
                processor.doQueryRaw();
            }

        } catch (Exception e) {
            catchException(query, e, Collections.singletonList(flowFile), context, session);
        }
    }

    protected abstract Dialect prepareDialect(final ProcessContext context);

    private void catchException(final Query query,
                                final Throwable e,
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

        getLogger().error(message, new Object[]{status, query}, e);
        context.yield();
    }

    private class QueryProcessor {
        private final List<FlowFile> flowFiles = new ArrayList<>();
        private final String org;
        private final long recordsPerFlowFile;
        private final Query query;
        private final ProcessContext context;
        private final ProcessSession session;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private final StopWatch stopWatch = new StopWatch();

        private long recordIndex = 0;

        private FlowFile flowFile;
        private RecordSetWriter writer;
        private OutputStream out;

        private QueryProcessor(final String org,
                               final Query query,
                               final long recordsPerFlowFile,
                               final FlowFile flowFile,
                               final boolean createdFlowFile, final ProcessContext context,
                               final ProcessSession session) {
            this.flowFiles.add(flowFile);
            if (recordsPerFlowFile == -1 || createdFlowFile) {
                this.flowFile = flowFile;
            } else {
                this.flowFile = session.create();
                this.flowFiles.add(this.flowFile);
            }
            this.org = org;
            this.recordsPerFlowFile = recordsPerFlowFile;
            this.query = query;
            this.context = context;
            this.session = session;

            this.stopWatch.start();
        }

        void doQuery() {
            try {

                getInfluxDBClient(context)
                        .getQueryApi()
                        .query(query, org, this::onResponseRecord, this::onError, this::onComplete);

                countDownLatch.await();
            } catch (Exception e) {
                catchException(query, e, flowFiles, context, session);
            }
        }

        void doQueryRaw() {
            try {

                getInfluxDBClient(context)
                        .getQueryApi()
                        .queryRaw(query, org, this::onResponseRaw, this::onError, this::onComplete);

                countDownLatch.await();
            } catch (Exception e) {
                catchException(query, e, flowFiles, context, session);
            }
        }

        private void onResponseRecord(final Cancellable cancellable, final FluxRecord fluxRecord) {

            if (fluxRecord == null) {
                return;
            }

            beforeOnResponse();

            Record record = toNifiRecord(fluxRecord);
            if (writer == null) {
                final RecordSchema recordSchema = record.getSchema();
                try {
                    out = session.write(flowFile);
                    writer = writerFactory.createWriter(getLogger(), recordSchema, out);
                    writer.beginRecordSet();
                } catch (Exception e) {
                    cancellable.cancel();
                    onError(e);
                }
            }

            try {
                writer.write(record);
            } catch (IOException e) {
                cancellable.cancel();
                onError(e);
            }
        }

        private void onResponseRaw(Cancellable cancellable, String record) {

            if (record == null || record.isEmpty()) {
                return;
            }

            beforeOnResponse();

            session.append(flowFile, out -> {
                if (recordIndex > 1) {
                    out.write('\n');
                }
                out.write(record.getBytes());
            });
        }

        private void beforeOnResponse() {
            recordIndex++;
            if (recordsPerFlowFile != -1 && recordIndex > recordsPerFlowFile) {
                closeRecordWriter();
                flowFile = session.create();
                flowFiles.add(flowFile);
                recordIndex = 1;
            }
        }

        private void onError(Throwable throwable) {
            stopWatch.stop();
            closeRecordWriter();

            catchException(query, throwable, flowFiles, context, session);

            countDownLatch.countDown();
        }

        private void onComplete() {
            stopWatch.stop();
            closeRecordWriter();

            session.transfer(flowFiles, REL_SUCCESS);

            for (FlowFile flowFile : flowFiles) {
                session.getProvenanceReporter()
                        .send(flowFile, influxDatabaseService.getDatabaseURL(), stopWatch.getElapsed(MILLISECONDS));
            }

            getLogger().debug("Query {} fetched in {}", new Object[]{query, stopWatch.getDuration()});

            countDownLatch.countDown();
        }

        private void closeRecordWriter() {
            if (writer != null) {
                try {
                    writer.finishRecordSet();
                    writer.close();
                    out.close();
                } catch (IOException e) {
                   throw new RuntimeException(e);
                }
            }

            out = null;
            writer = null;
        }

        private Record toNifiRecord(final FluxRecord fluxRecord) {

            Map<String, Object> values = new LinkedHashMap<>();
            List<RecordField> fields = new ArrayList<>();

            fluxRecord.getValues().forEach((fieldName, fluxValue) -> {

                if (fluxValue == null) {
                    return;
                }

                Object nifiValue = fluxValue;
                if (fluxValue instanceof Instant) {
                    nifiValue = java.sql.Timestamp.from((Instant) fluxValue);
                } else if (fluxValue instanceof Duration) {
                    nifiValue = ((Duration) fluxValue).get(ChronoUnit.NANOS);
                }

                DataType dataType = DataTypeUtils.inferDataType(nifiValue, RecordFieldType.STRING.getDataType());
                if (fluxValue.getClass().isArray()) {
                    dataType =  RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
                }

                fields.add(new RecordField(fieldName, dataType));
                values.put(fieldName, nifiValue);
            });

            return new MapRecord(new SimpleRecordSchema(fields), values);
        }
    }
}