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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdata.nifi.util.InfluxDBUtils;
import org.influxdata.nifi.util.PropertyValueUtils;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.util.InfluxDBUtils.TIMESTAMP_FIELD;

/**
 * @author Jakub Bednar (bednar@github) (17/07/2019 10:37)
 */
public class TestPutInfluxDatabaseRecordSettings_2 extends AbstractTestPutInfluxDatabaseRecord_2 {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void defaultSettingsValid() {

        runner.assertValid();
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testEmptyDBService() {
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyBucket() {
        runner.setProperty(PutInfluxDatabase_2.BUCKET, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyOrg() {
        runner.setProperty(PutInfluxDatabase_2.ORG, "");
        runner.assertNotValid();
    }

    @Test
    public void testPrecisionAllowedValues() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "MS");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "S");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "US");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "NS");
        runner.assertValid();

        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, "DAYS");
        runner.assertNotValid();
    }

    @Test
    public void testMaxRecordSizeZero() {
        runner.setProperty(MAX_RECORDS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void writeOptions() throws PropertyValueUtils.IllegalConfigurationException {

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        // Timestamp field
        Assert.assertEquals("timestamp", mapperOptions.getTimestamp());

        // Timestamp precision
        Assert.assertEquals(TimeUnit.NANOSECONDS, mapperOptions.getPrecision());

        // Measurement
        Assert.assertEquals("nifi-measurement", mapperOptions.getMeasurement());

        // Fields
        Assert.assertEquals(1, mapperOptions.getFields().size());
        Assert.assertEquals("nifi-field", mapperOptions.getFields().get(0));

        // Missing Fields
        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.IGNORE, mapperOptions.getMissingFields());

        // Tags
        Assert.assertEquals(1, mapperOptions.getTags().size());
        Assert.assertEquals("tags", mapperOptions.getTags().get(0));

        // Missing Tags
        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.IGNORE, mapperOptions.getMissingTags());

        // Complex fields behaviour
        Assert.assertEquals(InfluxDBUtils.ComplexFieldBehaviour.TEXT, mapperOptions.getComplexFieldBehaviour());

        // Null Field Behavior
        Assert.assertEquals(InfluxDBUtils.NullValueBehaviour.IGNORE, mapperOptions.getNullValueBehaviour());
    }

    @Test
    public void timestamp() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(TIMESTAMP_FIELD, "createdAt");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals("createdAt", mapperOptions.getTimestamp());
    }

    @Test
    public void timestampOverFlowFileAttributes() throws PropertyValueUtils.IllegalConfigurationException {

        ProcessSession processSession = runner.getProcessSessionFactory().createSession();

        FlowFile flowFile = processSession.create();

        Map<String, String> props = new HashMap<>();
        props.put("createdProperty", "createdTimestamp");

        flowFile = processSession.putAllAttributes(flowFile, props);

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "${createdProperty}");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), flowFile);

        Assert.assertEquals("createdTimestamp", mapperOptions.getTimestamp());
    }

    @Test
    public void timestampPrecision() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.TIMESTAMP_PRECISION, TimeUnit.MINUTES.name());

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(TimeUnit.MINUTES, mapperOptions.getPrecision());
    }

    @Test
    public void measurement() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.MEASUREMENT, "another-measurement");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals("another-measurement", mapperOptions.getMeasurement());
    }

    @Test
    public void measurementEmpty() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.MEASUREMENT, "");

        expectedException.expect(new AbstractTestPutInfluxDatabaseRecord.TypeOfExceptionMatcher<>(PropertyValueUtils.IllegalConfigurationException.class));

        processor.mapperOptions(runner.getProcessContext(), null);
    }

    @Test
    public void fields() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.FIELDS, "user-id, user-screen-name ");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(2, mapperOptions.getFields().size());
        Assert.assertEquals("user-id", mapperOptions.getFields().get(0));
        Assert.assertEquals("user-screen-name", mapperOptions.getFields().get(1));
    }

    @Test
    public void fieldsTrailingComma() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.FIELDS, "user-id, ");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(1, mapperOptions.getFields().size());
        Assert.assertEquals("user-id", mapperOptions.getFields().get(0));
    }

    @Test
    public void fieldsEmpty() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.FIELDS, " ");

        expectedException.expect(new AbstractTestPutInfluxDatabaseRecord.TypeOfExceptionMatcher<>(PropertyValueUtils.IllegalConfigurationException.class));

        processor.mapperOptions(runner.getProcessContext(), null);
    }

    @Test
    public void missingFields() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.FAIL, mapperOptions.getMissingFields());
    }

    @Test
    public void missingFieldsUnsupported() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, "wrong_name");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.IGNORE, mapperOptions.getMissingFields());
    }

    @Test
    public void tags() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.TAGS, "lang,keyword");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(2, mapperOptions.getTags().size());
        Assert.assertEquals("lang", mapperOptions.getTags().get(0));
        Assert.assertEquals("keyword", mapperOptions.getTags().get(1));
    }

    @Test
    public void missingTags() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.FAIL, mapperOptions.getMissingTags());
    }

    @Test
    public void complexFieldBehaviour() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.IGNORE.name());

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.ComplexFieldBehaviour.IGNORE, mapperOptions.getComplexFieldBehaviour());
    }

    @Test
    public void complexFieldBehaviourUnsupported() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, "wrong_name");

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.ComplexFieldBehaviour.TEXT, mapperOptions.getComplexFieldBehaviour());
    }

    @Test
    public void nullValueBehavior() throws PropertyValueUtils.IllegalConfigurationException {

        runner.setProperty(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());

        MapperOptions mapperOptions = processor.mapperOptions(runner.getProcessContext(), null);

        Assert.assertEquals(InfluxDBUtils.NullValueBehaviour.FAIL, mapperOptions.getNullValueBehaviour());
    }
}