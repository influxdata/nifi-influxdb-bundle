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
package org.influxdata.nifi.processors;

import java.io.ByteArrayOutputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.influxdata.client.WriteApi;
import org.influxdata.client.domain.WritePrecision;
import org.influxdata.client.write.events.AbstractWriteEvent;
import org.influxdata.client.write.events.WriteErrorEvent;
import org.influxdata.client.write.events.WriteRetriableErrorEvent;
import org.influxdata.exceptions.InfluxException;
import org.influxdata.nifi.util.PropertyValueUtils;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.CHARSET;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.INFLUX_DB_FAIL_TO_INSERT;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_FAILURE;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_MAX_SIZE_EXCEEDED;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_RETRY;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_SUCCESS;

/**
 * @author Jakub Bednar (bednar@github) (11/07/2019 07:44)
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"influxdb", "measurement", "insert", "write", "put", "timeseries", "2.0"})
@CapabilityDescription("Processor to write the content of a FlowFile in 'line protocol'. Please check details of the 'line protocol' in InfluxDB 2.0 documentation (https://www.influxdb.com/). "
        + " The flow file can contain single measurement point or multiple measurement points separated by line separator."
        + " The timestamp precision is defined by Timestamp property.")
@WritesAttributes({
        @WritesAttribute(attribute = INFLUX_DB_ERROR_MESSAGE, description = "InfluxDB error message"),
})
public class PutInfluxDatabase_2 extends AbstractInfluxDatabaseProcessor_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {

        final Set<Relationship> relationships = new LinkedHashSet<>();

        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
        relationships.add(REL_MAX_SIZE_EXCEEDED);

        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        propertyDescriptors.add(INFLUX_DB_SERVICE);
        propertyDescriptors.add(BUCKET);
        propertyDescriptors.add(ORG);
        propertyDescriptors.add(TIMESTAMP_PRECISION);

        propertyDescriptors.add(ENABLE_GZIP);
        propertyDescriptors.add(LOG_LEVEL);

        propertyDescriptors.add(CHARSET);
        propertyDescriptors.add(MAX_RECORDS_SIZE);

        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (flowFile.getSize() == 0) {
            getLogger().error("Empty measurements");
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, "Empty measurements");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (flowFile.getSize() > maxRecordsSize) {
            getLogger().error("Message size of records exceeded {} max allowed is {}", new Object[]{flowFile.getSize(), maxRecordsSize});
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, "Max records size exceeded " + flowFile.getSize());
            session.transfer(flowFile, REL_MAX_SIZE_EXCEEDED);
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String bucket = context.getProperty(BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        String org = context.getProperty(ORG).evaluateAttributeExpressions(flowFile).getValue();
        WritePrecision precision = PropertyValueUtils.getEnumValue(WritePrecision.class, null,
                context.getProperty(TIMESTAMP_PRECISION).evaluateAttributeExpressions(flowFile).getValue());

        try {

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                session.exportTo(flowFile, baos);

                String records = new String(baos.toByteArray(), charset);
                write(context, bucket, org, precision, records);
                stopWatch.stop();

                getLogger().debug("Records {} inserted", new Object[]{records});

                session.transfer(flowFile, REL_SUCCESS);

                // Provenance report
                String message = String.format("Added %d points to InfluxDB.", flowFile.getSize());
                session.getProvenanceReporter().send(flowFile, influxDatabaseService.getDatabaseURL(), message, stopWatch.getElapsed(MILLISECONDS));
            }
        } catch (InfluxException ie) {
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, String.valueOf(ie.getMessage()));

            // retryable error
            if (Arrays.asList(429, 503).contains(ie.status()) || ie.getCause() instanceof SocketTimeoutException) {
                getLogger().error("Failed to insert into influxDB due {} to {} and retrying",
                        new Object[]{ie.status(), ie.getLocalizedMessage()}, ie);
                session.transfer(flowFile, REL_RETRY);
            } else {
                getLogger().error(INFLUX_DB_FAIL_TO_INSERT, new Object[]{ie.getLocalizedMessage()}, ie);
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        } catch (Exception e) {
            getLogger().error(INFLUX_DB_FAIL_TO_INSERT, new Object[]{e.getLocalizedMessage()}, e);
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, String.valueOf(e.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private void write(ProcessContext context, final String bucket, final String org, final WritePrecision precision, final String record) {

        List<AbstractWriteEvent> events = new ArrayList<>();
        try (WriteApi writeApi = getInfluxDBClient(context).getWriteApi()) {

            writeApi.listenEvents(WriteErrorEvent.class, events::add);
            writeApi.listenEvents(WriteRetriableErrorEvent.class, events::add);

            writeApi.writeRecord(bucket, org, precision, record);
        }

        events.forEach(event -> {
            if (event instanceof WriteRetriableErrorEvent) {
                throw new InfluxException(((WriteRetriableErrorEvent) event).getThrowable());
            } else if (event instanceof WriteErrorEvent) {
                throw new InfluxException(((WriteErrorEvent) event).getThrowable());
            }
        });
    }
}