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

import java.lang.reflect.Constructor;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.influxdata.nifi.util.InfluxDBUtils;

import avro.shaded.com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Test;

import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE_LOG;
import static org.influxdata.nifi.processors.PutInfluxDatabaseRecord.DATABASE_NAME_EMPTY_MESSAGE;
import static org.influxdata.nifi.util.InfluxDBUtils.AT_LEAST_ONE_FIELD_DEFINED_MESSAGE;
import static org.influxdata.nifi.util.InfluxDBUtils.MEASUREMENT_NAME_EMPTY_MESSAGE;
import static org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour.FAIL;
import static org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour.IGNORE;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class TestPutInfluxDatabaseRecordErrorHandling extends AbstractTestPutInfluxDatabaseRecord {

    @Test
    public void databaseNameNotDefined() {

        testRunner.setProperty(PutInfluxDatabaseRecord.DB_NAME, "${databaseProperty}");

        prepareData();

        // empty database name
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("databaseProperty", "");

        testRunner.enqueue("", attributes);
        testRunner.run();

        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(DATABASE_NAME_EMPTY_MESSAGE, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void measurementNotDefined() {

        testRunner.setProperty(InfluxDBUtils.MEASUREMENT, "${measurementProperty}");

        prepareData();

        // empty measurement
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("measurementProperty", "");

        testRunner.enqueue("", attributes);
        testRunner.run();

        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(MEASUREMENT_NAME_EMPTY_MESSAGE,
                failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void fieldsNotDefined() {

        testRunner.setProperty(InfluxDBUtils.FIELDS, "${fieldsProperty}");

        prepareData();

        // not defined fields
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("fieldsProperty", ",,");

        testRunner.enqueue("", attributes);
        testRunner.run();

        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(AT_LEAST_ONE_FIELD_DEFINED_MESSAGE,
                failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void databaseNotFound() {

        errorToRetryRelationship(InfluxDBException.DatabaseNotFoundException.class, "database not found");
    }

    @Test
    public void authorizationFailException() {

        errorToRetryRelationship(InfluxDBException.AuthorizationFailedException.class, "authorization failed");
    }

    @Test
    public void cacheSizeExceeded() {

        errorToRetryRelationship(
                InfluxDBException.CacheMaxMemorySizeExceededException.class,
                "cache-max-memory-size exceeded");
    }

    @Test
    public void fieldTypeConflict() {

        errorToFailureRelationship(InfluxDBException.FieldTypeConflictException.class, "field type conflict");
    }

    @Test
    public void pointsBeyondRetentionPolicy() {

        errorToFailureRelationship(InfluxDBException.PointsBeyondRetentionPolicyException.class,
                "points beyond retention policy");
    }

    @Test
    public void retryBufferOverrun() {

        errorToFailureRelationship(InfluxDBException.RetryBufferOverrunException.class, "Retry buffer overrun");
    }

    @Test
    public void unableToParse() {

        errorToFailureRelationship(InfluxDBException.UnableToParseException.class, "unable to parse");
    }

    @Test
    public void hintedHandOffQueueNotEmpty() {

        InfluxDBException exception = createInfluxException(InfluxDBException.HintedHandOffQueueNotEmptyException.class,
                "hinted handoff queue not empty");

        yieldProcessor("hinted handoff queue not empty", exception);
    }

    @Test
    public void influxDBIOException() {

        SocketTimeoutException ioException = new SocketTimeoutException("timeout");

        yieldProcessor(ioException.toString(), new InfluxDBIOException(ioException));
    }

    @Test
    public void unknownHostExceptionToRetry() {

        writeAnswer = invocation -> {
            throw new UnknownHostException("influxdb");
        };

        errorToRelationship("influxdb", PutInfluxDatabaseRecord.REL_RETRY, false);

        MockProcessContext processContext = (MockProcessContext) testRunner.getProcessContext();

        Assert.assertTrue(processContext.isYieldCalled());
    }

    @Test
    public void unknownHostExceptionToRetryInner() {

        writeAnswer = invocation -> {
            throw new RuntimeException(new UnknownHostException("influxdb"));
        };

        errorToRelationship("java.net.UnknownHostException: influxdb", PutInfluxDatabaseRecord.REL_RETRY, false);

        MockProcessContext processContext = (MockProcessContext) testRunner.getProcessContext();

        Assert.assertTrue(processContext.isYieldCalled());
    }

    @Test
    public void missingFieldFail() {

        testRunner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-field-missing");
        testRunner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, FAIL.name());

        prepareData();

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_FAILURE);

        Assert.assertEquals(1, failures.size());

        String message = String.format(InfluxDBUtils.REQUIRED_FIELD_MISSING, "nifi-field-missing");
        Assert.assertEquals(message, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void missingFieldIgnore() {

        testRunner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-field-missing");
        testRunner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, IGNORE.name());

        prepareData();

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> successes = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_SUCCESS);

        Assert.assertEquals(1, successes.size());
        Assert.assertNull(successes.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void missingTagIgnore() {

        testRunner.setProperty(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.IGNORE.name());
        testRunner.setProperty(InfluxDBUtils.TAGS, "nifi-tag,nifi-tag-missing");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value");

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> successes = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_SUCCESS);
        Assert.assertEquals(1, successes.size());

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());
    }

    @Test
    public void missingTagFail() {

        testRunner.setProperty(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());
        testRunner.setProperty(InfluxDBUtils.TAGS, "nifi-tag,nifi-tag-missing");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value");

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PutInfluxDatabaseRecord.REL_FAILURE);
        Assert.assertEquals(1, failures.size());

        String message = String.format(InfluxDBUtils.REQUIRED_FIELD_MISSING, "nifi-tag-missing");
        Assert.assertEquals(message, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void errorRelationshipHasErrorInLog() {

        errorToFailureRelationship(InfluxDBException.UnableToParseException.class, "unable to parse");

        List<LogMessage> errors = logger.getErrorMessages();

        // First is formatted message, Second Stack Trace
        Assert.assertEquals(2, errors.size());

        Assert.assertTrue(errors.get(0).getMsg().contains(INFLUX_DB_ERROR_MESSAGE_LOG));
        Assert.assertTrue(errors.get(1).getThrowable() instanceof InfluxDBException.UnableToParseException);
    }

    @Test
    public void retryRelationshipHasErrorInLog() {

        errorToRetryRelationship(InfluxDBException.AuthorizationFailedException.class, "authorization failed");

        List<LogMessage> errors = logger.getErrorMessages();

        // First is formatted message, Second Stack Trace
        Assert.assertEquals(2, errors.size());

        Assert.assertTrue(errors.get(0).getMsg().contains(INFLUX_DB_ERROR_MESSAGE_LOG));
        Assert.assertTrue(errors.get(1).getThrowable() instanceof InfluxDBException.AuthorizationFailedException);
    }

    private void errorToRetryRelationship(@NonNull final Class<? extends InfluxDBException> exceptionType,
                                          @NonNull final String exceptionMessage) {

        writeAnswer = invocation -> {
            throw createInfluxException(exceptionType, exceptionMessage);
        };

        errorToRelationship(exceptionMessage, PutInfluxDatabaseRecord.REL_RETRY, true);
    }

    private void errorToFailureRelationship(@NonNull final Class<? extends InfluxDBException> exceptionType,
                                            @NonNull final String exceptionMessage) {

        writeAnswer = invocation -> {
            throw createInfluxException(exceptionType, exceptionMessage);
        };

        errorToRelationship(exceptionMessage, PutInfluxDatabaseRecord.REL_FAILURE, false);
    }

    private void yieldProcessor(@NonNull final String exceptionMessage,
                                @NonNull final InfluxDBException influxException) {

        writeAnswer = invocation -> {
            throw influxException;
        };

        errorToRelationship(exceptionMessage, PutInfluxDatabaseRecord.REL_RETRY, false);

        MockProcessContext processContext = (MockProcessContext) testRunner.getProcessContext();

        Assert.assertTrue(processContext.isYieldCalled());
    }

    private void errorToRelationship(@NonNull final String exceptionMessage,
                                     @NonNull final Relationship relationship,
                                     @NonNull final Boolean isPenalized) {

        prepareData();

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(relationship, 1);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(relationship).get(0);
        Assert.assertEquals(exceptionMessage, flowFile.getAttribute(INFLUX_DB_ERROR_MESSAGE));

        // Is Penalized
        Assert.assertEquals(isPenalized, testRunner.getPenalizedFlowFiles().contains(flowFile));
    }

    @NonNull
    private InfluxDBException createInfluxException(@NonNull final Class<? extends InfluxDBException> exceptionType,
                                                    @NonNull final String message) {

        Assert.assertNotNull(exceptionType);
        Assert.assertNotNull(message);

        try {

            Constructor<? extends InfluxDBException> constructor = exceptionType.getDeclaredConstructor(String.class);

            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance(message);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }

    private void prepareData() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");
    }
}
