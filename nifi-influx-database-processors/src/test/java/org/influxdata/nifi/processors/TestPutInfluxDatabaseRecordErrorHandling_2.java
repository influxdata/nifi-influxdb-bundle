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

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import avro.shaded.com.google.common.collect.Maps;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.exceptions.NotFoundException;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor_2;
import org.influxdata.nifi.util.InfluxDBUtils;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.processors.Utils.createErrorResponse;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_RETRY_AFTER;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_RETRY;
import static org.influxdata.nifi.util.InfluxDBUtils.AT_LEAST_ONE_FIELD_DEFINED_MESSAGE;
import static org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour.FAIL;
import static org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour.IGNORE;
import static org.junit.Assert.assertEquals;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 07:24)
 */
public class TestPutInfluxDatabaseRecordErrorHandling_2 extends AbstractTestPutInfluxDatabaseRecord_2 {

    @Test
    public void bucketNotDefined() {

        runner.setProperty(PutInfluxDatabaseRecord_2.BUCKET, "${bucketProperty}");

        prepareData();

        // empty database name
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("bucketProperty", "");

        runner.enqueue("", attributes);
        runner.run();

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(AbstractInfluxDatabaseProcessor_2.BUCKET_NAME_EMPTY_MESSAGE, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void orgNotDefined() {

        runner.setProperty(PutInfluxDatabaseRecord_2.ORG, "${orgProperty}");

        prepareData();

        // empty measurement
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("orgProperty", "");

        runner.enqueue("", attributes);
        runner.run();

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(AbstractInfluxDatabaseProcessor_2.ORG_NAME_EMPTY_MESSAGE,
                failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void fieldsNotDefined() {

        runner.setProperty(InfluxDBUtils.FIELDS, "${fieldsProperty}");

        prepareData();

        // not defined fields
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("fieldsProperty", ",,");

        runner.enqueue("", attributes);
        runner.run();

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        Assert.assertEquals(1, failures.size());
        Assert.assertEquals(AT_LEAST_ONE_FIELD_DEFINED_MESSAGE,
                failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void socketTimeoutException() {

        errorToRetryRelationship(new InfluxException(new SocketTimeoutException("Simulate error: SocketTimeoutException")), "Simulate error: SocketTimeoutException");
    }

    @Test
    public void serviceUnavailableException() {

        errorToRetryRelationship(new InfluxException(createErrorResponse(503)), "Simulate error: 503");
    }

    @Test
    public void tooManyRequestsException() {

        errorToRetryRelationship(
                new InfluxException(createErrorResponse(429)), "Simulate error: 429");
    }

    @Test
    public void tooManyRequestsExceptionRetryAfterHeader() {

        Map<String, String> headers = Collections.singletonMap("Retry-After", "145");

        errorToRetryRelationship(
                new InfluxException(createErrorResponse(429, headers)), "Simulate error: 429");

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_RETRY);

        assertEquals("145", flowFiles.get(0).getAttribute(INFLUX_DB_RETRY_AFTER));
    }

    @Test
    public void notFoundException() {

        errorToFailureRelationship(new NotFoundException(createErrorResponse(404)), "Simulate error: 404");
    }

    @Test
    public void missingFieldFail() {

        runner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-field-missing");
        runner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, FAIL.name());

        prepareData();

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        Assert.assertEquals(1, failures.size());

        String message = String.format(InfluxDBUtils.REQUIRED_FIELD_MISSING, "nifi-field-missing");
        Assert.assertEquals(message, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void missingFieldIgnore() {

        runner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-field-missing");
        runner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, IGNORE.name());

        prepareData();

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> successes = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        Assert.assertEquals(1, successes.size());
        Assert.assertNull(successes.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void missingTagIgnore() {

        runner.setProperty(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.IGNORE.name());
        runner.setProperty(InfluxDBUtils.TAGS, "nifi-tag,nifi-tag-missing");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value");

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> successes = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);
        Assert.assertEquals(1, successes.size());

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());
    }

    @Test
    public void missingTagFail() {

        runner.setProperty(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());
        runner.setProperty(InfluxDBUtils.TAGS, "nifi-tag,nifi-tag-missing");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value");

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);
        Assert.assertEquals(1, failures.size());

        String message = String.format(InfluxDBUtils.REQUIRED_FIELD_MISSING, "nifi-tag-missing");
        Assert.assertEquals(message, failures.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void errorRelationshipHasErrorInLog() {

        errorToFailureRelationship(new InfluxException("unable to parse"), "unable to parse");

        List<LogMessage> errors = logger.getErrorMessages();

        // First is formatted message, Second Stack Trace
        Assert.assertEquals(1, errors.size());

        Assert.assertTrue(errors.get(0).getMsg().contains("Failed to insert into influxDB due"));
		Assert.assertEquals("com.influxdb.exceptions.InfluxException: unable to parse", errors.get(0).getArgs()[2]);
    }

    @Test
    public void retryRelationshipHasErrorInLog() {

        errorToRetryRelationship(new InfluxException(createErrorResponse(503)), "Simulate error: 503");

        List<LogMessage> errors = logger.getErrorMessages();

        // First is formatted message, Second Stack Trace
        Assert.assertEquals(1, errors.size());

        Assert.assertTrue(errors.get(0).getMsg().contains("Failed to insert into influxDB due "));
        Assertions.assertThat(errors.get(0).getArgs()[3].toString()).startsWith("com.influxdb.exceptions.InfluxException");
        Assertions.assertThat(errors.get(0).getArgs()[3].toString()).endsWith("Simulate error: 503");
    }

    private void errorToRetryRelationship(@NonNull final InfluxException exception,
                                          @NonNull final String exceptionMessage) {

        writeAnswer = invocation -> {
            throw exception;
        };

        errorToRelationship(exceptionMessage, AbstractInfluxDatabaseProcessor.REL_RETRY, true);
    }

    private void errorToFailureRelationship(@NonNull final InfluxException exception,
                                            @NonNull final String exceptionMessage) {

        writeAnswer = invocation -> {
            throw exception;
        };

        errorToRelationship(exceptionMessage, AbstractInfluxDatabaseProcessor.REL_FAILURE, false);
    }


    private void errorToRelationship(@NonNull final String exceptionMessage,
                                     @NonNull final Relationship relationship,
                                     @NonNull final Boolean isPenalized) {

        prepareData();

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(relationship, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(relationship).get(0);
        Assertions.assertThat(flowFile.getAttribute(INFLUX_DB_ERROR_MESSAGE)).endsWith(exceptionMessage);

        // Is Penalized
        Assert.assertEquals(isPenalized, runner.getPenalizedFlowFiles().contains(flowFile));
    }

    private void prepareData() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");
    }
}