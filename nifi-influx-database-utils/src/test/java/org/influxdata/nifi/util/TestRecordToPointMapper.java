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
package org.influxdata.nifi.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.influxdata.nifi.processors.MapperOptions;
import org.influxdata.nifi.processors.RecordToPointMapper;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Jakub Bednar (bednar@github) (03/06/2019 09:02)
 */
public class TestRecordToPointMapper {

    private RecordToPointMapper mapper;
    private SimpleRecordSchema schema;

    @Before
    public void before()
    {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("tag1", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("tag2", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("tag3", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("tag4", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.BIGINT.getDataType(),
                RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("tag5", RecordFieldType.MAP.getDataType()));
        fields.add(new RecordField("field1", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("field2", RecordFieldType.BOOLEAN.getDataType()));
        fields.add(new RecordField("field3", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("field4", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("field5", RecordFieldType.BIGINT.getDataType()));
        fields.add(new RecordField("field6", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("field7", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("field8", RecordFieldType.BIGINT.getDataType()));
        fields.add(new RecordField("field9", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType())));
        fields.add(new RecordField("field10", RecordFieldType.MAP.getDataType()));
        fields.add(new RecordField("field11", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("timestamp", RecordFieldType.LONG.getDataType()));

        schema = new SimpleRecordSchema(fields);
    }

    @Test
    public void toPoint() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void nullRecord() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        List<Point> points = mapper.mapRecord(null);

        Assert.assertEquals(0, points.size());
    }

    @Test
    public void nullValues() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02 field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i 123456789", points.get(0).lineProtocol());
    }

    @Test(expected = IllegalStateException.class)
    public void missingFieldFail() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "fieldMissing"))
                .missingFields(InfluxDBUtils.MissingItemsBehaviour.FAIL)
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void complexField() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field9"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field9", new Object[]{25L, 55L});
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field9=\"[25, 55]\" 123456789", points.get(0).lineProtocol());
    }

    @Test(expected = IllegalStateException.class)
    public void complexFieldFail() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field9"))
                .tags(Arrays.asList("tag1", "tag2"))
                .complexFieldBehaviour(InfluxDBUtils.ComplexFieldBehaviour.FAIL)
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field9", new Object[]{25L, 55L});
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        mapper.mapRecord(record);
    }

    @Test
    public void complexFieldIgnore() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field9"))
                .tags(Arrays.asList("tag1", "tag2"))
                .complexFieldBehaviour(InfluxDBUtils.ComplexFieldBehaviour.IGNORE)
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field9", new Object[]{25L, 55L});
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void complexFieldWarn() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field9"))
                .tags(Arrays.asList("tag1", "tag2"))
                .complexFieldBehaviour(InfluxDBUtils.ComplexFieldBehaviour.WARN)
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field9", new Object[]{25L, 55L});
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void map() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field10"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        Map<String, Object> mapValues = new HashMap<>();
        mapValues.put("float", 55.5F);
        mapValues.put("long", 150L);
        mapValues.put("boolean", true);
        mapValues.put("string", "string value");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field10", mapValues);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 boolean=true,field1=25.0,field2=true,float=55.5,long=150.0,string=\"string value\" 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void timestampFieldNull() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp2");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i", points.get(0).lineProtocol());
    }

    @Test
    public void timestampValueNull() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);
        values.put("timestamp", null);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i", points.get(0).lineProtocol());
    }


    @Test
    public void timestampDate() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("tag1");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i 123456789000000", points.get(0).lineProtocol());
    }

    @Test
    public void tagFieldNull() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"))
                .tags(Arrays.asList("tag1", "tag2", "tagNotExist"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", null);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");
        values.put("field7", 8);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02 field1=25.0,field2=true,field3=10000i,field4=50.54999923706055,field5=963258741i,field6=\"description\",field7=8i 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void tagComplex() {
        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2"))
                .tags(Arrays.asList("tag1", "tag2", "tag3", "tag4", "tag5"))
                .timestamp("timestamp");

        Map<String, Object> mapValues = new HashMap<>();
        mapValues.put("float", 55.5F);
        mapValues.put("long", 150L);
        mapValues.put("boolean", true);
        mapValues.put("string", "string value");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("tag3", new Object[]{"sub-value1", "sub-value2"});
        values.put("tag4", BigInteger.valueOf(1324567899));
        values.put("tag5", mapValues);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,boolean=true,float=55.5,long=150,string=string\\ value,tag1=1970-01-02,tag2=555,tag3=[sub-value1\\,\\ sub-value2],tag4=1324567899 field1=25.0,field2=true 123456789", points.get(0).lineProtocol());
    }

    @Test
    public void choice() {

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2", "field3", "field11"))
                .tags(Arrays.asList("tag1", "tag2", "tagNotExist"))
                .timestamp("timestamp");

        mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", null);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field11", 888888L);
        values.put("timestamp", 123456789);

        final Record record = new MapRecord(schema, values);
        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02 field1=25.0,field11=888888i,field2=true,field3=10000i 123456789", points.get(0).lineProtocol());
    }
}