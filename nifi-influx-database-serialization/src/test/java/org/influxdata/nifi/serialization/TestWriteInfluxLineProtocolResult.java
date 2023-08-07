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
package org.influxdata.nifi.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.influxdata.nifi.processors.MapperOptions;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * @author Jakub Bednar (bednar@github) (30/05/2019 10:47)
 */
public class TestWriteInfluxLineProtocolResult {

    @Test
    public void testDataTypes() throws IOException, ParseException {
        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            if (fieldType == RecordFieldType.CHOICE) {
                final List<DataType> possibleTypes = new ArrayList<>();
                possibleTypes.add(RecordFieldType.INT.getDataType());
                possibleTypes.add(RecordFieldType.LONG.getDataType());

                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getChoiceDataType(possibleTypes)));
            } else if (fieldType == RecordFieldType.MAP) {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getMapDataType(RecordFieldType.INT.getDataType())));
            } else {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
            }
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("gmt"));
        final long time = df.parse("2017/01/01 17:00:00.000").getTime();

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("height", 48);
        map.put("width", 96);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("string", "string");
        valueMap.put("boolean", true);
        valueMap.put("byte", (byte) 1);
        valueMap.put("char", 'c');
        valueMap.put("short", (short) 8);
        valueMap.put("int", 9);
        valueMap.put("bigint", BigInteger.valueOf(8L));
        valueMap.put("long", 8L);
        valueMap.put("float", 8.0F);
        valueMap.put("double", 8.0D);
        valueMap.put("date", new Date(time));
        valueMap.put("time", new Time(time));
        valueMap.put("timestamp", new Timestamp(time));
        valueMap.put("record", null);
        valueMap.put("array", null);
        valueMap.put("choice", 48L);
        valueMap.put("map", map);

        final Record record = new MapRecord(schema, valueMap);
        final RecordSet rs = RecordSet.of(schema, record);

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .tags(Arrays.asList("string"))
                .fields(Arrays.asList("boolean", "byte", "char", "short", "int", "bigint", "long", "float", "double", "date", "time", "timestamp", "record", "array", "choice", "map"));

        try (final WriteInfluxLineProtocolResult writer = new WriteInfluxLineProtocolResult(baos, schema,
                Mockito.mock(ComponentLog.class), options, StandardCharsets.UTF_8.name())) {

            writer.write(rs);
        }

        final String output = baos.toString();

        assertEquals("measurement-test,string=string bigint=8i,boolean=true,byte=1i,char=\"c\",choice=48i,date=1483290000000i,double=8.0,float=8.0,height=48i,int=9i,long=8i,short=8i,time=1483290000000i,timestamp=1483290000000i,width=96i\n", output);
    }

    @Test
    public void testTimestampWithNullFormat() throws IOException {
        final Map<String, Object> values = new HashMap<>();
        values.put("timestamp", new java.sql.Timestamp(37293723L));
        values.put("time", new java.sql.Time(37293723L));
        values.put("date", new java.sql.Date(37293723L));
        values.put("location", "west");

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("location", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(schema, values);
        final RecordSet rs = RecordSet.of(schema, record);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .timestamp("timestamp")
                .tags(Arrays.asList("location"))
                .fields(Arrays.asList("time", "date"));

        try (final WriteInfluxLineProtocolResult writer = new WriteInfluxLineProtocolResult(baos, schema,
                Mockito.mock(ComponentLog.class), options, StandardCharsets.UTF_8.name())) {

            writer.write(rs);
        }

        final String output = baos.toString();

        assertEquals("measurement-test,location=west date=37293723i,time=37293723i 37293723000000\n", output);
    }

    @Test
    public void testExtraFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("location", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("amount", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        values.put("location", "up");
        values.put("amount", 10);
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .tags(Arrays.asList("location", "id"))
                .fields(Arrays.asList("amount"));

        try (final WriteInfluxLineProtocolResult writer = new WriteInfluxLineProtocolResult(baos, schema,
                Mockito.mock(ComponentLog.class), options, StandardCharsets.UTF_8.name())) {

            writer.write(record);
        }

        final String output = baos.toString();

        assertEquals("measurement-test,id=1,location=up amount=10i\n", output);
    }

    @Test
    public void testMissingFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("location", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("amount", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("amount", 10);
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .tags(Arrays.asList("location", "id"))
                .fields(Arrays.asList("amount"));

        try (final WriteInfluxLineProtocolResult writer = new WriteInfluxLineProtocolResult(baos, schema,
                Mockito.mock(ComponentLog.class), options, StandardCharsets.UTF_8.name())) {

            writer.write(record);
        }

        final String output = baos.toString();

        assertEquals("measurement-test,id=1 amount=10i\n", output);
    }

    @Test
    public void testMultipleRecords() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("location", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("amount", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values1 = new LinkedHashMap<>();
        values1.put("id", "1");
        values1.put("location", "west");
        values1.put("amount", 10);

        final Map<String, Object> values2 = new LinkedHashMap<>();
        values2.put("id", "2");
        values2.put("location", "north");
        values2.put("amount", 20);

        final Map<String, Object> values3 = new LinkedHashMap<>();
        values3.put("id", "3");
        values3.put("amount", 35);

        final RecordSet rs = RecordSet.of(schema, new MapRecord(schema, values1), new MapRecord(schema, values2), new MapRecord(schema, values3));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .tags(Arrays.asList("location", "id"))
                .fields(Arrays.asList("amount"));

        try (final WriteInfluxLineProtocolResult writer = new WriteInfluxLineProtocolResult(baos, schema,
                Mockito.mock(ComponentLog.class), options, StandardCharsets.UTF_8.name())) {

            writer.write(rs);
        }

        final String output = baos.toString();

        assertEquals("measurement-test,id=1,location=west amount=10i\n"
                + "measurement-test,id=2,location=north amount=20i\n"
                + "measurement-test,id=3 amount=35i\n", output);
    }
}
