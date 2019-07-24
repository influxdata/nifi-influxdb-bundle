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

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.influxdata.query.FluxRecord;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.junit.Assert;
import org.junit.Test;

import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_SUCCESS;

/**
 * @author Jakub Bednar (24/07/2019 09:00)
 */
public class TestGetInfluxDatabaseRecord_2 extends AbstractTestGetInfluxDatabaseRecord_2 {

    @Test
    public void success() {

        FluxRecord fluxRecord = new FluxRecord(0);
        fluxRecord.getValues().put("bool_value", true);
        fluxRecord.getValues().put("long_value", 123456789123456789L);
        fluxRecord.getValues().put("double_value", 123456789.123456789D);
        fluxRecord.getValues().put("base64Binary_value", "binary".getBytes());
        fluxRecord.getValues().put("instant_value", Instant.ofEpochSecond(123456789));
        fluxRecord.getValues().put("duration_value", Duration.ofNanos(10_000L));
        fluxRecord.getValues().put("string_value", "InfluxDB rocks!");
        fluxRecord.getValues().put("null_value", null);

        queryOnResponseRecords.add(fluxRecord);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        Assert.assertEquals(1, writer.getRecordsWritten().size());

        Record nifiRecord = writer.getRecordsWritten().get(0);

        RecordSchema schema = nifiRecord.getSchema();
        Assert.assertEquals(7, schema.getFieldCount());
        Assert.assertEquals(RecordFieldType.BOOLEAN, schema.getField("bool_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.LONG, schema.getField("long_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.DOUBLE, schema.getField("double_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.ARRAY, schema.getField("base64Binary_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.BYTE, ((ArrayDataType) schema.getField("base64Binary_value").get().getDataType()).getElementType().getFieldType());
        Assert.assertEquals(RecordFieldType.DATE, schema.getField("instant_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.LONG, schema.getField("duration_value").get().getDataType().getFieldType());
        Assert.assertEquals(RecordFieldType.STRING, schema.getField("string_value").get().getDataType().getFieldType());

        Assert.assertEquals(true, nifiRecord.getValue("bool_value"));
        Assert.assertEquals(123456789123456789L, nifiRecord.getValue("long_value"));
        Assert.assertEquals(123456789.123456789D, nifiRecord.getValue("double_value"));
        Assert.assertArrayEquals("binary".getBytes(), (byte[]) nifiRecord.getValue("base64Binary_value"));
        Assert.assertEquals(Date.from(Instant.ofEpochSecond(123456789)), nifiRecord.getValue("instant_value"));
        Assert.assertEquals(10000L, nifiRecord.getValue("duration_value"));
        Assert.assertEquals("InfluxDB rocks!", nifiRecord.getValue("string_value"));
    }

    @Test
    public void moreRecords() {

        FluxRecord fluxRecord1 = new FluxRecord(0);
        fluxRecord1.getValues().put("value", 1L);

        FluxRecord fluxRecord2 = new FluxRecord(0);
        fluxRecord2.getValues().put("value", "2");

        queryOnResponseRecords.add(fluxRecord1);
        queryOnResponseRecords.add(fluxRecord2);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        Assert.assertEquals(2, writer.getRecordsWritten().size());

        // Record 1
        Record nifiRecord = writer.getRecordsWritten().get(0);
        RecordSchema schema = nifiRecord.getSchema();

        Assert.assertEquals(1, schema.getFieldCount());
        Assert.assertEquals(RecordFieldType.LONG, schema.getField("value").get().getDataType().getFieldType());
        Assert.assertEquals(1L, nifiRecord.getValue("value"));

        // Record 2
        nifiRecord = writer.getRecordsWritten().get(1);
        schema = nifiRecord.getSchema();

        Assert.assertEquals(1, schema.getFieldCount());
        Assert.assertEquals(RecordFieldType.STRING, schema.getField("value").get().getDataType().getFieldType());
        Assert.assertEquals("2", nifiRecord.getValue("value"));
    }

    @Test
    public void moreTables() {
        FluxRecord fluxRecord1 = new FluxRecord(0);
        fluxRecord1.getValues().put("value", 1L);

        FluxRecord fluxRecord2 = new FluxRecord(1);
        fluxRecord2.getValues().put("value", "2");

        queryOnResponseRecords.add(fluxRecord1);
        queryOnResponseRecords.add(fluxRecord2);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        Assert.assertEquals(2, writer.getRecordsWritten().size());

        // Record 1
        Record nifiRecord = writer.getRecordsWritten().get(0);
        RecordSchema schema = nifiRecord.getSchema();

        Assert.assertEquals(1, schema.getFieldCount());
        Assert.assertEquals(RecordFieldType.LONG, schema.getField("value").get().getDataType().getFieldType());
        Assert.assertEquals(1L, nifiRecord.getValue("value"));

        // Record 2
        nifiRecord = writer.getRecordsWritten().get(1);
        schema = nifiRecord.getSchema();

        Assert.assertEquals(1, schema.getFieldCount());
        Assert.assertEquals(RecordFieldType.STRING, schema.getField("value").get().getDataType().getFieldType());
        Assert.assertEquals("2", nifiRecord.getValue("value"));
    }

    @Test
    public void useMoreFlowFiles() {

        FluxRecord fluxRecord1 = new FluxRecord(0);
        fluxRecord1.getValues().put("value", 1L);

        FluxRecord fluxRecord2 = new FluxRecord(0);
        fluxRecord2.getValues().put("value", 2L);

        FluxRecord fluxRecord3 = new FluxRecord(0);
        fluxRecord3.getValues().put("value", 3L);

        FluxRecord fluxRecord4 = new FluxRecord(0);
        fluxRecord4.getValues().put("value", 4L);

        FluxRecord fluxRecord5 = new FluxRecord(0);
        fluxRecord5.getValues().put("value", 5L);

        FluxRecord fluxRecord6 = new FluxRecord(0);
        fluxRecord6.getValues().put("value", 6L);

        queryOnResponseRecords.add(fluxRecord1);
        queryOnResponseRecords.add(fluxRecord2);
        queryOnResponseRecords.add(fluxRecord3);
        queryOnResponseRecords.add(fluxRecord4);
        queryOnResponseRecords.add(fluxRecord5);
        queryOnResponseRecords.add(fluxRecord6);

        runner.setProperty(GetInfluxDatabase_2.RECORDS_PER_FLOWFILE, "2");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 4);
        Assert.assertEquals(6, writer.getRecordsWritten().size());

        for (int i = 0; i < 6; i++) {
            Record nifiRecord = writer.getRecordsWritten().get(i);
            RecordSchema schema = nifiRecord.getSchema();

            Assert.assertEquals(RecordFieldType.LONG, schema.getField("value").get().getDataType().getFieldType());
            Assert.assertEquals((long) i + 1, nifiRecord.getValue("value"));
        }

    }

    @Test
    public void containsProvenanceReport() {

        runner.enqueue("");
        runner.run();

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();

        Assert.assertEquals(1, events.size());

        ProvenanceEventRecord record = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, record.getEventType());
        Assert.assertEquals(processor.getIdentifier(), record.getComponentId());
        Assert.assertEquals("http://localhost:8086", record.getTransitUri());
    }
}
