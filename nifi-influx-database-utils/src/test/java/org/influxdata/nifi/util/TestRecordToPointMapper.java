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
import org.apache.nifi.serialization.record.RecordSchema;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author Jakub Bednar (bednar@github) (03/06/2019 09:02)
 */
public class TestRecordToPointMapper {

    @Test
    public void toPoint() {

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("tag1", RecordFieldType.DATE.getDataType()));
        fields.add(new RecordField("tag2", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("field1", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("field2", RecordFieldType.BOOLEAN.getDataType()));
        fields.add(new RecordField("field3", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("field4", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("field5", RecordFieldType.BIGINT.getDataType()));
        fields.add(new RecordField("field6", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        MapperOptions options = new MapperOptions()
                .measurement("measurement-test")
                .fields(Arrays.asList("field1", "field2"))
                .tags(Arrays.asList("tag1", "tag2"))
                .timestamp("timestamp");

        RecordToPointMapper mapper = new RecordToPointMapper(options, schema, Mockito.mock(ComponentLog.class));

        final Map<String, Object> values = new HashMap<>();
        values.put("tag1", new Date(123456789));
        values.put("tag2", 555);
        values.put("field1", 25D);
        values.put("field2", true);
        values.put("field3", 10000L);
        values.put("field4", 50.55F);
        values.put("field5", BigInteger.valueOf(963258741));
        values.put("field6", "description");

        final Record record = new MapRecord(schema, values);

        List<Point> points = mapper.mapRecord(record);

        Assert.assertEquals(1, points.size());
        Assert.assertEquals("measurement-test,tag1=1970-01-02,tag2=555 field1=25.0,field2=true", points.get(0).lineProtocol());
    }
}