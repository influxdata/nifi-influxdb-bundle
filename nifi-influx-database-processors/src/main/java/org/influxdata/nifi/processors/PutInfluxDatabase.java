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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.util.PropertyValueUtils;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBIOException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"influxdb", "measurement","insert", "write", "put", "timeseries"})
@CapabilityDescription("Processor to write the content of a FlowFile in 'line protocol'.  Please check details of the 'line protocol' in InfluxDB documentation (https://www.influxdb.com/). "
        + "  The flow file can contain single measurement point or multiple measurement points separated by line separator."
        + " The timestamp precision is defined by Timestamp property. If you do not specify precision then the InfluxDB assumes that timestamps are in nanoseconds.")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE, description = "InfluxDB error message"),
    })
public class PutInfluxDatabase extends AbstractInfluxDatabaseProcessor {

    public static AllowableValue CONSISTENCY_LEVEL_ALL = new AllowableValue("ALL", "All", "Return success when all nodes have responded with write success");
    public static AllowableValue CONSISTENCY_LEVEL_ANY = new AllowableValue("ANY", "Any", "Return success when any nodes have responded with write success");
    public static AllowableValue CONSISTENCY_LEVEL_ONE = new AllowableValue("ONE", "One", "Return success when one node has responded with write success");
    public static AllowableValue CONSISTENCY_LEVEL_QUORUM = new AllowableValue("QUORUM", "Quorum", "Return success when a majority of nodes have responded with write success");

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("influxdb-consistency-level")
            .displayName("Consistency Level")
            .description("InfluxDB consistency level")
            .required(true)
            .defaultValue(CONSISTENCY_LEVEL_ONE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues(CONSISTENCY_LEVEL_ONE, CONSISTENCY_LEVEL_ANY, CONSISTENCY_LEVEL_ALL, CONSISTENCY_LEVEL_QUORUM)
            .build();

    public static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("influxdb-retention-policy")
            .displayName("Retention Policy")
            .description("Retention policy for the saving the records")
            .defaultValue("autogen")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMESTAMP_PRECISION = new PropertyDescriptor.Builder()
            .name("influxdb-timestamp-precision")
            .displayName("Timestamp precision")
            .description("The precision of the time stamps. InfluxDB assumes that timestamps are in nanoseconds if you do not specify precision.")
            .required(false)
            .allowableValues(Arrays.stream(TimeUnit.values()).filter(tu -> !TimeUnit.DAYS.equals(tu)).map(Enum::name).toArray(String[]::new))
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> tempRelationships = new HashSet<>();
        tempRelationships.add(REL_SUCCESS);
        tempRelationships.add(REL_FAILURE);
        tempRelationships.add(REL_RETRY);
        tempRelationships.add(REL_MAX_SIZE_EXCEEDED);
        relationships = Collections.unmodifiableSet(tempRelationships);

        final List<PropertyDescriptor> tempDescriptors = new ArrayList<>();
        tempDescriptors.add(DB_NAME);
        tempDescriptors.add(INFLUX_DB_URL);
        tempDescriptors.add(INFLUX_DB_CONNECTION_TIMEOUT);
        tempDescriptors.add(USERNAME);
        tempDescriptors.add(PASSWORD);
        tempDescriptors.add(CHARSET);
        tempDescriptors.add(CONSISTENCY_LEVEL);
        tempDescriptors.add(RETENTION_POLICY);
        tempDescriptors.add(TIMESTAMP_PRECISION);
        tempDescriptors.add(MAX_RECORDS_SIZE);
        tempDescriptors.add(INFLUX_DB_CLIENT_TYPE);
        propertyDescriptors = Collections.unmodifiableList(tempDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        maxRecordsSize = context.getProperty(MAX_RECORDS_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if ( flowFile.getSize() == 0) {
            getLogger().error("Empty measurements");
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, "Empty measurement size " + flowFile.getSize());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if ( flowFile.getSize() > maxRecordsSize) {
            getLogger().error("Message size of records exceeded {} max allowed is {}", new Object[] { flowFile.getSize(), maxRecordsSize});
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, "Max records size exceeded " + flowFile.getSize());
            session.transfer(flowFile, REL_MAX_SIZE_EXCEEDED);
            return;
        }

        Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());
        String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).evaluateAttributeExpressions(flowFile).getValue();
        String database = context.getProperty(DB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String retentionPolicy = context.getProperty(RETENTION_POLICY).evaluateAttributeExpressions(flowFile).getValue();
        TimeUnit precision = PropertyValueUtils.getEnumValue(TimeUnit.class, null,
                context.getProperty(TIMESTAMP_PRECISION).evaluateAttributeExpressions(flowFile).getValue());

        try {
            long startTimeMillis = System.currentTimeMillis();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            String records = new String(baos.toByteArray(), charset);

            writeToInfluxDB(context, consistencyLevel, database, retentionPolicy, precision, records);

            final long endTimeMillis = System.currentTimeMillis();
            getLogger().debug("Records {} inserted", new Object[] {records});

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile,
                    new StringBuilder("influxdb://").append(context.getProperty(INFLUX_DB_URL).evaluateAttributeExpressions().getValue()).append("/").append(database).toString(),
                    (endTimeMillis - startTimeMillis));
        } catch (InfluxDBIOException exception) {
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            if ( exception.getCause() instanceof SocketTimeoutException ) {
                getLogger().error("Failed to insert into influxDB due SocketTimeoutException to {} and retrying",
                        new Object[]{exception.getLocalizedMessage()}, exception);
                session.transfer(flowFile, REL_RETRY);
            } else {
                getLogger().error(INFLUX_DB_FAIL_TO_INSERT,
                        new Object[]{exception.getLocalizedMessage()}, exception);
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        } catch (Exception exception) {
            getLogger().error(INFLUX_DB_FAIL_TO_INSERT,
                    new Object[]{exception.getLocalizedMessage()}, exception);
            flowFile = session.putAttribute(flowFile, INFLUX_DB_ERROR_MESSAGE, String.valueOf(exception.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    protected void writeToInfluxDB(ProcessContext context, String consistencyLevel, String database, String retentionPolicy, final TimeUnit precision, String records) {
        if (precision != null) {
            getInfluxDB(context).write(database, retentionPolicy, InfluxDB.ConsistencyLevel.valueOf(consistencyLevel), precision, records);
        } else {
            getInfluxDB(context).write(database, retentionPolicy, InfluxDB.ConsistencyLevel.valueOf(consistencyLevel), records);
        }
    }
}