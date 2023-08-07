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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.influxdb.impl.TimeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.nifi.serialization.record.RecordFieldType.LONG;
import static org.apache.nifi.serialization.record.RecordFieldType.MAP;
import static org.apache.nifi.serialization.record.RecordFieldType.STRING;

public class TestInfluxLineProtocolRecordReader extends AbstractTestInfluxLineProtocolReader {

    @Test
    public void schemaNotNull() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertNotNull(schema);
    }

    @Test
    public void recordNotNull() throws MalformedRecordException, IOException, SchemaNotFoundException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        Record record = recordReader.nextRecord();
        Assertions.assertNotNull(record);

        // next record null
        record = recordReader.nextRecord();
        Assertions.assertNull(record);
    }

    @Test
    public void recordMultiple() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather temperature=82 1465839830100400200" + System.lineSeparator()
                + "weather temperature=80 1465839830100450200";
        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Assertions.assertNotNull(record);

        record = recordReader.nextRecord();
        Assertions.assertNotNull(record);
    }

    @Test
    public void closeInputStream() throws SchemaNotFoundException, MalformedRecordException, IOException {

        InputStream spiedInputStream = Mockito.spy(toInputData());

        RecordReader recordReader = readerFactory.createRecordReader(variables, spiedInputStream, -1, logger);
        recordReader.close();

        Mockito.verify(spiedInputStream, Mockito.times(1)).close();
    }

    @Test
    public void emptyData() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(" "), -1, logger);

        Assertions.assertNull(recordReader.nextRecord());
    }

    @Test
    public void measurement() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(STRING, schema.getDataType(InfluxLineProtocolRecordReader.MEASUREMENT).get().getFieldType());

        Record record = recordReader.nextRecord();
        Assertions.assertEquals("weather", record.getValue(InfluxLineProtocolRecordReader.MEASUREMENT));
    }

    @Test
    public void measurementWithEscapedSpaces() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "wea\\ ther,location=us-midwest temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Assertions.assertEquals("wea ther", record.getValue(InfluxLineProtocolRecordReader.MEASUREMENT));
    }

    @Test
    public void measurementWithEscapedComma() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "wea\\,ther,location=us-midwest temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Assertions.assertEquals("wea,ther", record.getValue(InfluxLineProtocolRecordReader.MEASUREMENT));
    }

    @Test
    public void tags() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.TAG_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertEquals(1, tags.size());
        Assertions.assertEquals("us-midwest", tags.get("location"));
    }

    @Test
    public void tagsEmpty() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.TAG_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertTrue(tags.isEmpty());
    }

    @Test
    public void tagsMultiple() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest,season=summer temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.TAG_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertEquals(2, tags.size());
        Assertions.assertEquals("us-midwest", tags.get("location"));
        Assertions.assertEquals("summer", tags.get("season"));
    }

    @Test
    public void tagWithEscapedComma() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us\\,midwest temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertEquals(1, tags.size());
        Assertions.assertEquals("us,midwest", tags.get("location"));
    }

    @Test
    public void tagWithEscapedEqual() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,loca\\=tion=us-midwest temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertEquals(1, tags.size());
        Assertions.assertEquals("us-midwest", tags.get("loca=tion"));
    }

    @Test
    public void tagWithEscapedSpace() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us\\ midwest temperature=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);

        Assertions.assertEquals(1, tags.size());
        Assertions.assertEquals("us midwest", tags.get("location"));
    }

    @Test
    public void fields() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(82f, fields.get("temperature"));
    }

    @Test
    public void fieldsEmpty() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        MalformedRecordException malformedRecordException = Assertions.assertThrows(MalformedRecordException.class, recordReader::nextRecord);
        MatcherAssert.assertThat(malformedRecordException.getMessage(), Matchers.startsWith("Not parsable data: 'weather 1465839830100400200'."));
    }

    @Test
    public void fieldsWrongFormatEmpty() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather value 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        MalformedRecordException malformedRecordException = Assertions.assertThrows(MalformedRecordException.class, recordReader::nextRecord);
        MatcherAssert.assertThat(malformedRecordException.getMessage(), Matchers.startsWith("Not parsable data: 'weather value 1465839830100400200'."));
    }

    @Test
    public void fieldFloat() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather value=82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(82f, fields.get("value"));
    }

    @Test
    public void fieldInteger() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather value=83i 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(83L, fields.get("value"));
    }

    @Test
    public void fieldIntegerLargeThenInteger() throws MalformedRecordException, IOException, SchemaNotFoundException {

        String data = "disk,device=disk1s1,fstype=apfs,host=pikachu.local,mode=rw,path=/ "
                + "total=250685575168i,free=63410728960i,used=183377002496i,used_percent=74.30555863296408,"
                + "inodes_total=9223372036854775807i,inodes_free=9223372036853280100i,inodes_used=1495707i "
                + "1525932900000000000";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();

        Assertions.assertEquals(5, ((Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET)).size());

        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);
        Assertions.assertEquals(7, fields.size());
        Assertions.assertEquals(250685575168L, fields.get("total"));
    }

    @Test
    public void fieldString() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather value=\"84\" 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals("84", fields.get("value"));
    }

    @Test
    public void fieldBoolean() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather true1=t,true2=T,true3=true,true4=True,true5=TRUE,"
                + "false1=f,false2=F,false3=false,false4=False,false5=FALSE 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(10, fields.size());
        Assertions.assertEquals(true, fields.get("true1"));
        Assertions.assertEquals(true, fields.get("true2"));
        Assertions.assertEquals(true, fields.get("true3"));
        Assertions.assertEquals(true, fields.get("true4"));
        Assertions.assertEquals(true, fields.get("true5"));
        Assertions.assertEquals(false, fields.get("false1"));
        Assertions.assertEquals(false, fields.get("false2"));
        Assertions.assertEquals(false, fields.get("false3"));
        Assertions.assertEquals(false, fields.get("false4"));
        Assertions.assertEquals(false, fields.get("false5"));
    }

    @Test
    public void fieldOnly() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather value=84";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(MAP, schema.getDataType(InfluxLineProtocolRecordReader.FIELD_SET).get().getFieldType());

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(84f, fields.get("value"));

        Map tags = (Map) record.getValue(InfluxLineProtocolRecordReader.TAG_SET);
        Assertions.assertTrue(tags.isEmpty());
        Assertions.assertNull(record.getValue(InfluxLineProtocolRecordReader.TIMESTAMP));
    }

    @Test
    public void fieldNotParsable() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=\"82 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        MalformedRecordException malformedRecordException = Assertions.assertThrows(MalformedRecordException.class, recordReader::nextRecord);
        Assertions.assertEquals("Not parsable data: 'weather,location=us-midwest temperature=\"82 1465839830100400200'.", malformedRecordException.getMessage());
    }

    @Test
    public void fieldWithEscapedComma() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82,measure=\"Cel\\,sius\" 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("Cel\\,sius", fields.get("measure"));
    }

    @Test
    public void fieldWithEscapedEqual() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82,measure=\"Cel\\=sius\" 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("Cel\\=sius", fields.get("measure"));
    }

    @Test
    public void fieldWithEscapedSpace() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82,measure=\"Cel\\ sius\" 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("Cel\\ sius", fields.get("measure"));
    }

    @Test
    public void fieldWithSpace1() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "system,host=pikachu.local uptime_format=\"7 days,  5:38\" 1525951350000000000";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals("7 days,  5:38", fields.get("uptime_format"));
    }

    @Test
    public void fieldWithSpace2() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "system,host=tomcat2 uptime_format=\" 2:25\" 1526037880000000000";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(1, fields.size());
        Assertions.assertEquals(" 2:25", fields.get("uptime_format"));
    }

    @Test
    public void fieldWithEscapedQuotes() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82,measure=\"Cel\\\\\"sius\" 1465839830100400200";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        Record record = recordReader.nextRecord();
        Map fields = (Map) record.getValue(InfluxLineProtocolRecordReader.FIELD_SET);

        Assertions.assertEquals(2, fields.size());
        Assertions.assertEquals("Cel\\\"sius", fields.get("measure"));
    }

    @Test
    public void timestamp() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(LONG, schema.getDataType(InfluxLineProtocolRecordReader.TIMESTAMP).get().getFieldType());

        Record record = recordReader.nextRecord();
        Assertions.assertEquals(1465839830100400200L, record.getValue(InfluxLineProtocolRecordReader.TIMESTAMP));
    }

    @Test
    public void timestampInRFC3339() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82 2016-10-31T06:52:20.020Z";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(LONG, schema.getDataType(InfluxLineProtocolRecordReader.TIMESTAMP).get().getFieldType());

        Record record = recordReader.nextRecord();
        String influxFormat = TimeUtil.toInfluxDBTimeFormat((Long) record.getValue(InfluxLineProtocolRecordReader.TIMESTAMP));

        Assertions.assertEquals("2016-10-31T06:52:20.020Z", influxFormat);
    }

    @Test
    public void timestampInWrongFormat() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82 wrong_format";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        MalformedRecordException malformedRecordException = Assertions.assertThrows(MalformedRecordException.class, recordReader::nextRecord);
        Assertions.assertEquals("Not parsable data: 'weather,location=us-midwest temperature=82 wrong_format'.", malformedRecordException.getMessage());
    }

    @Test
    public void timestampEmpty() throws SchemaNotFoundException, MalformedRecordException, IOException {

        String data = "weather,location=us-midwest temperature=82";

        RecordReader recordReader = readerFactory.createRecordReader(variables, toInputData(data), -1,logger);

        RecordSchema schema = recordReader.getSchema();
        Assertions.assertEquals(LONG, schema.getDataType(InfluxLineProtocolRecordReader.TIMESTAMP).get().getFieldType());

        Record record = recordReader.nextRecord();
        Assertions.assertNull(record.getValue(InfluxLineProtocolRecordReader.TIMESTAMP));
    }

    @Nullable
    private InputStream toInputData() {

        return toInputData("weather,location=us-midwest temperature=82 1465839830100400200");
    }

    @Nullable
    private InputStream toInputData(@Nullable final String inlineProtocol) {

        if (inlineProtocol == null) {
            return null;
        }

        return IOUtils.toInputStream(inlineProtocol, StandardCharsets.UTF_8);
    }

    public static class ReaderProcessor extends AbstractProcessor {

        private static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
                .name("record-reader")
                .identifiesControllerService(RecordReaderFactory.class)
                .required(true)
                .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(RECORD_READER);

            return descriptors;
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        }
    }
}
