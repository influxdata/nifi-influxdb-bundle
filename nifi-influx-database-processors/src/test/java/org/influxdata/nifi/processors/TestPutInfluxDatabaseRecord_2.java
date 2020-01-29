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

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.util.InfluxDBUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.util.InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 07:58)
 */
public class TestPutInfluxDatabaseRecord_2 extends AbstractTestPutInfluxDatabaseRecord_2 {

    @Test
    public void withoutFlowFile() {

        runner.run();

        Mockito.verify(mockWriteApi, Mockito.never()).writePoints(Mockito.any(), Mockito.any(), Mockito.any());

        List<Point> allValues = getAllValues();
        Assert.assertTrue(allValues.isEmpty());
    }

    @Test
    public void measurementValue() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        runner.enqueue("");
        runner.run();

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals("nifi-measurement", getMeasurement(points.get(0)));
    }

    @Test
    public void measurementValueByFieldValue() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-measurement", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", "measurement-name");

        runner.enqueue("");
        runner.run();

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals("measurement-name", getMeasurement(points.get(0)));
    }

    @Test
    public void addDateField() {

        Date fieldValue = new Date();

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.DATE);
    }

    @Test
    public void addTimeField() {

        Time fieldValue = new Time(System.currentTimeMillis());

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.TIME);
    }

    @Test
    public void addTimestampField() {

        Timestamp fieldValue = new Timestamp(System.currentTimeMillis());

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.TIMESTAMP);
    }

    @Test
    public void addDoubleField() {

        double fieldValue = 123456.78d;

        addFieldByType(fieldValue, RecordFieldType.DOUBLE);
    }

    @Test
    public void addFloatField() {

        float fieldValue = -123456.78f;

        addFieldByType(fieldValue, RecordFieldType.FLOAT);
    }

    @Test
    public void addLongField() {

        long fieldValue = 987654321L;

        addFieldByType(fieldValue, RecordFieldType.LONG);
    }

    @Test
    public void addIntField() {

        int fieldValue = 123;

        addFieldByType(fieldValue, RecordFieldType.INT);
    }

    @Test
    public void addByteField() {

        byte fieldValue = (byte) 5;

        addFieldByType(fieldValue, 5, RecordFieldType.BYTE);
    }

    @Test
    public void addShortField() {

        short fieldValue = (short) 100;

        addFieldByType(fieldValue, 100, RecordFieldType.SHORT);
    }

    @Test
    public void addBigIntField() {

        BigInteger fieldValue = new BigInteger("75000");

        addFieldByType(fieldValue, RecordFieldType.BIGINT);
    }

    @Test
    public void addBooleanField() {

        Boolean fieldValue = Boolean.TRUE;

        //noinspection ConstantConditions
        addFieldByType(fieldValue, RecordFieldType.BOOLEAN);
    }

    @Test
    public void addStringField() {

        String fieldValue = "string value";

        addFieldByType(fieldValue, RecordFieldType.STRING);
    }

    @Test
    public void addNullField() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord((String) null);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(0, points.size());
    }

    @Test
    public void addFieldChar() {

        char fieldValue = 'x';

        addFieldByType(fieldValue, String.valueOf(fieldValue), RecordFieldType.CHAR);
    }

    @Test
    public void multipleFields() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record-1");
        recordReader.addRecord("nifi-record-2");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(2, points.size());

        Assert.assertEquals("nifi-record-1", getField("nifi-field", points.get(0)));
        Assert.assertEquals("nifi-record-2", getField("nifi-field", points.get(1)));
    }

    @Test
    public void addMapFields() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.MAP);

        Map<String, Object> fields = new HashMap<>();
        fields.put("nifi-float", 55.5F);
        fields.put("nifi-long", 150L);
        fields.put("nifi-boolean", true);
        fields.put("nifi-string", "string value");

        recordReader.addRecord(fields);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(55.5F, getField("nifi-float", points.get(0)));
        Assert.assertEquals(150L, getField("nifi-long", points.get(0)));
        Assert.assertEquals(true, getField("nifi-boolean", points.get(0)));
        Assert.assertEquals("string value", getField("nifi-string", points.get(0)));
    }

    @Test
    public void addChoiceField() {

        DataType choiceDataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.STRING.getDataType());

        addFieldByType("15", 15L, choiceDataType);
    }

    @Test
    public void addArrayFieldText() {

        runner.setProperty(COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.TEXT.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, "[25, 55]", dataType);
    }

    @Test
    public void addArrayFieldIgnore() {

        runner.setProperty(COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.IGNORE.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addArrayFieldWarn() {

        runner.setProperty(COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; skipping"));
    }

    @Test
    public void addArrayFieldFail() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.FAIL.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(2, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; routing to failure"));
    }

    @Test
    public void addRecordFieldText() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.TEXT.name());

        addFieldRecordType(true, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addRecordFieldIgnore() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.IGNORE.name());

        addFieldRecordType(false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addRecordFieldWarn() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());

        addFieldRecordType(false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; skipping"));
    }

    @Test
    public void addRecordFieldFail() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.FAIL.name());

        addFieldRecordType(false, AbstractInfluxDatabaseProcessor.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(2, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; routing to failure"));
    }

    @Test
    public void timestampNotDefined() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertNull(getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.NS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefined() {

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", 789456123L);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(789456123L), getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.NS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedButEmpty() {

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", null);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertNull(getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.NS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedAsString() {

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "156");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(156), getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.NS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedAsDate() {

        Date dateValue = new Date();

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.DATE);
        recordReader.addRecord("nifi-record", dateValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(dateValue.getTime()), getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.MS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampPrecisionDefined() {

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");
        runner.setProperty(PutInfluxDatabaseRecord_2.TIMESTAMP_PRECISION, WritePrecision.S.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", 123456L);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(123456L), getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.S, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampPrecisionIgnoredForDate() {

        Date dateValue = new Date();

        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "nifi-timestamp");
        runner.setProperty(PutInfluxDatabaseRecord_2.TIMESTAMP_PRECISION, WritePrecision.S.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", dateValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(dateValue.getTime()), getTimestamp(points.get(0)));
        Assert.assertEquals(WritePrecision.MS, getTimeUnit(points.get(0)));
    }

    @Test
    public void withoutTags() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(0, tags.size());
    }

    @Test
    public void addStringTag() {

        addTagByType("nifi-tag-value", "nifi-tag-value", RecordFieldType.STRING);
    }

    @Test
    public void addNumberTag() {

        addTagByType(123L, "123", RecordFieldType.LONG);
    }

    @Test
    public void addBooleanTag() {

        addTagByType(true, "true", RecordFieldType.BOOLEAN);
    }

    @Test
    public void addDateTag() {

        Instant instant = Instant.ofEpochMilli(0L);

        addTagByType(Date.from(instant), "1970-01-01", RecordFieldType.DATE);
    }

    @Test
    public void addMoreTags() {

        runner.setProperty(InfluxDBUtils.TAGS, "nifi-tag1,nifi-tag2");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag1", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag2", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value1", "tag-value2");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(2, tags.size());
        Assert.assertEquals("tag-value1", tags.get("nifi-tag1"));
        Assert.assertEquals("tag-value2", tags.get("nifi-tag2"));
    }

    @Test
    public void addMapTags() {

        runner.setProperty(InfluxDBUtils.TAGS, "nifi-tags");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tags", RecordFieldType.MAP);

        Map<String, String> tags = new HashMap<>();
        tags.put("nifi-tag1", "tag-value1");
        tags.put("nifi-tag2", "tag-value2");

        recordReader.addRecord("nifi-record", tags);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue();
        Assert.assertEquals(1, points.size());

        Map<String, String> pointTags = getTags(points.get(0));
        Assert.assertNotNull(pointTags);
        Assert.assertEquals(2, pointTags.size());
        Assert.assertEquals("tag-value1", pointTags.get("nifi-tag1"));
        Assert.assertEquals("tag-value2", pointTags.get("nifi-tag2"));
    }

    @Test
    public void addChoiceTag() {

        DataType choiceDataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.STRING.getDataType());

        addTagByType("15", "15", choiceDataType);
    }

    @Test
    public void addArrayTagText() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.TEXT.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, "[25, 55]", dataType);
    }

    @Test
    public void addArrayTagIgnore() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.IGNORE.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addArrayTagWarn() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; skipping"));
    }

    @Test
    public void addArrayTagFail() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.FAIL.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, AbstractInfluxDatabaseProcessor.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(2, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; routing to failure"));
    }

    @Test
    public void addRecordTagText() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.TEXT.name());

        addTagRecordType(true, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addRecordTagIgnore() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.IGNORE.name());

        addTagRecordType(false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    @Test
    public void addRecordTagWarn() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());

        addTagRecordType(false, AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; skipping"));
    }

    @Test
    public void addRecordTagFail() {

        runner.setProperty(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.FAIL.name());

        addTagRecordType(false, AbstractInfluxDatabaseProcessor.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(2, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; routing to failure"));
    }

    private void addTagRecordType(@NonNull Boolean isValueExpected,
                                  @NonNull Relationship expectedRelations) {

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("sub-record-field", RecordFieldType.BOOLEAN.getDataType()));

        SimpleRecordSchema childSchema = new SimpleRecordSchema(fields);
        DataType dataType = RecordFieldType.RECORD.getRecordDataType(childSchema);

        Map<String, Object> subRecordValues = new HashMap<>();
        subRecordValues.put("sub-record-field", true);

        addTagByType(subRecordValues, "{sub-record-field=true}", dataType, isValueExpected, expectedRelations);
    }

    @Test
    public void containsProvenanceReport() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        runner.enqueue("");
        runner.run();

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();

        Assert.assertEquals(1, events.size());

        ProvenanceEventRecord record = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, record.getEventType());
        Assert.assertEquals(processor.getIdentifier(), record.getComponentId());
        Assert.assertEquals("http://localhost:8086/", record.getTransitUri());
    }

    @Test
    public void nullValueBehaviourIgnoredTag() {

        runner.setProperty(InfluxDBUtils.TAGS, "nifi-not-exist-tag");
        runner.setProperty(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.IGNORE.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        Map<String, String> tags = getTags(pointCapture.getValue().get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(0, tags.size());
    }

    @Test
    public void nullValueBehaviourFailTag() {

        runner.setProperty(InfluxDBUtils.TAGS, "nifi-not-exist-tag");
        runner.setProperty(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertEquals("Cannot write FlowFile to InfluxDB because the field 'nifi-not-exist-tag' has null value.",
                flowFiles.get(0).getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void nullValueBehaviourIgnoredField() {

        runner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-not-exist-field");
        runner.setProperty(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.IGNORE.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertNull(getField("nifi-not-exist-field", pointCapture.getValue().get(0)));
    }

    @Test
    public void nullValueBehaviourFailField() {

        runner.setProperty(InfluxDBUtils.FIELDS, "nifi-field,nifi-not-exist-field");
        runner.setProperty(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertEquals("Cannot write FlowFile to InfluxDB because the field 'nifi-not-exist-field' has null value.",
                flowFiles.get(0).getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE));
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @NonNull final RecordFieldType tagType) {
        addTagByType(tagValue, expectedValue, tagType.getDataType());
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @Nullable final DataType dataType) {

        addTagByType(tagValue, expectedValue, dataType, true, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @Nullable final DataType dataType,
                              @NonNull final Boolean isValueExpected,
                              @NonNull final Relationship expectedRelations) {

        runner.setProperty(InfluxDBUtils.TAGS, "nifi-tag");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", dataType.getFieldType());
        recordReader.addRecord("nifi-record", tagValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelations, 1);

        List<Point> points = getAllValues();
        if (AbstractInfluxDatabaseProcessor.REL_FAILURE.equals(expectedRelations)) {

            Assert.assertEquals(0, points.size());

            return;
        }

        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);

        if (isValueExpected) {
            Assert.assertEquals(1, tags.size());
            Assert.assertEquals(expectedValue, tags.get("nifi-tag"));
        } else {
            Assert.assertEquals(0, tags.size());
        }
    }

    private void addFieldRecordType(@NonNull final Boolean isValueExpected, @NonNull final Relationship relationship) {

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("sub-record-field", RecordFieldType.BOOLEAN.getDataType()));

        SimpleRecordSchema childSchema = new SimpleRecordSchema(fields);
        DataType dataType = RecordFieldType.RECORD.getRecordDataType(childSchema);

        Map<String, Object> subRecordValues = new HashMap<>();
        subRecordValues.put("sub-record-field", true);

        addFieldByType(subRecordValues, "{sub-record-field=true}", dataType, isValueExpected, relationship);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @NonNull final RecordFieldType fieldType) {

        addFieldByType(fieldValue, fieldValue, fieldType);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @NonNull final RecordFieldType fieldType) {

        addFieldByType(fieldValue, expectedValue, fieldType.getDataType());
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @Nullable final DataType dataType) {

        addFieldByType(fieldValue, expectedValue, dataType, true, AbstractInfluxDatabaseProcessor.REL_SUCCESS);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @Nullable final DataType dataType,
                                @NonNull final Boolean isValueExpected,
                                @NonNull final Relationship expectedRelations) {

        try {
            //PR missing method #addSchemaField(final String fieldName, final DataType type) in MockRecordReader
            //recordReader.addSchemaField("nifi-field", dataType.getFieldType());

            Field field = recordReader.getClass().getDeclaredField("fields");
            field.setAccessible(true);
            List<RecordField> recordFields = (List<RecordField>) field.get(recordReader);
            RecordField recordField = new RecordField("nifi-field", dataType, RecordField.DEFAULT_NULLABLE);
            recordFields.add(recordField);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        recordReader.addRecord(fieldValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(expectedRelations, 1);

        List<Point> points = getAllValues();
        if (isValueExpected) {
            Assert.assertEquals(1, points.size());
            Object field = getField("nifi-field", points.get(0));
            Assert.assertEquals(expectedValue, field);
        } else {
            Assert.assertEquals(0, points.size());
        }
    }

    @Nullable
    private String getMeasurement(@NonNull final Point point) {

        return accessFieldValue(point, "name");

    }

    @Nullable
    private Long getTimestamp(@NonNull final Point point) {

        return accessFieldValue(point, "time");
    }

    @Nullable
    private WritePrecision getTimeUnit(@NonNull final Point point) {

        return accessFieldValue(point, "precision");
    }

    @Nullable
    private Map<String, String> getTags(@NonNull final Point point) {

        return accessFieldValue(point, "tags");
    }

    @Nullable
    private Object getField(@NonNull final String fieldName,
                            @NonNull final Point point) {

        Map<String, Object> fields = accessFieldValue(point, "fields");
        assert fields != null;

        return fields.get(fieldName);

    }

    @Nullable
    private <T> T accessFieldValue(@NonNull final Point point, @NonNull final String fieldName) {

        try {
            //noinspection unchecked
            return (T) FieldUtils.readField(point, fieldName, true);

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Point> getAllValues() {
        return pointCapture.getAllValues().stream().flatMap(List::stream).collect(Collectors.toList());
    }
}