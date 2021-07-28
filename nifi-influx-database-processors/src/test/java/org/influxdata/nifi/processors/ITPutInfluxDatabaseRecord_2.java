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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunners;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.serialization.InfluxLineProtocolReader;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.influxdata.nifi.util.InfluxDBUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_URL;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 11:26)
 */
public class ITPutInfluxDatabaseRecord_2 extends AbstractITInfluxDB_2 {

    private MockRecordParser recordReader;

    @Before
    public void setUp() throws Exception {

        init();

        PutInfluxDatabaseRecord_2 processor = new PutInfluxDatabaseRecord_2();
        runner = TestRunners.newTestRunner(processor);
		runner.setValidateExpressionUsage(false);
        runner.setProperty(PutInfluxDatabaseRecord_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(AbstractInfluxDatabaseProcessor.RECORD_READER_FACTORY, "recordReader");
        runner.setProperty(InfluxDBUtils.MEASUREMENT, "testRecordMeasurement");
        runner.setProperty(PutInfluxDatabaseRecord_2.BUCKET, bucketName);
        runner.setProperty(PutInfluxDatabaseRecord_2.ORG, "my-org");

        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_URL, INFLUX_DB_2);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);
        processor.initialize(initContext);
    }

    @Test
    public void storeComplexData() {

        runner.setProperty(InfluxDBUtils.TAGS, "lang,keyword");
        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "timestamp");
        runner.setProperty(InfluxDBUtils.FIELDS,
                "retweet_count,tweet_id,followers_count,screen_name,friends_count,favourites_count,user_verified,raw");

        recordReader.addSchemaField("lang", RecordFieldType.STRING);
        recordReader.addSchemaField("keyword", RecordFieldType.STRING);
        recordReader.addSchemaField("retweet_count", RecordFieldType.INT);
        recordReader.addSchemaField("tweet_id", RecordFieldType.STRING);
        recordReader.addSchemaField("followers_count", RecordFieldType.INT);
        recordReader.addSchemaField("screen_name", RecordFieldType.STRING);
        recordReader.addSchemaField("friends_count", RecordFieldType.INT);
        recordReader.addSchemaField("favourites_count", RecordFieldType.INT);
        recordReader.addSchemaField("user_verified", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("raw", RecordFieldType.STRING);
        recordReader.addSchemaField("timestamp", RecordFieldType.LONG);

        Date timeValue = new Date();
        recordReader.addRecord(
                "en", "crypto", 10, "123456789id", 15, "my name", 5, 20, false, "crypto rules!", timeValue);

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        FluxRecord record = tables.get(0).getRecords().get(0);

        Assert.assertEquals("testRecordMeasurement", record.getMeasurement());
        Assert.assertEquals("en", record.getValueByKey("lang"));
        Assert.assertEquals("crypto", record.getValueByKey("keyword"));
        Assert.assertEquals("123456789id", record.getValueByKey("tweet_id"));
        Assert.assertEquals(15L, record.getValueByKey("followers_count"));
        Assert.assertEquals("my name", record.getValueByKey("screen_name"));
        Assert.assertEquals(5L, record.getValueByKey("friends_count"));
        Assert.assertEquals(20L, record.getValueByKey("favourites_count"));
        Assert.assertEquals(false, record.getValueByKey("user_verified"));

        long millis = TimeUnit.MILLISECONDS.convert(timeValue.getTime(), TimeUnit.MILLISECONDS);
        Assert.assertEquals(Instant.ofEpochMilli(millis), record.getTime());
    }

    @Test
    public void fieldTypeConflict() throws InitializationException {

        runner.setProperty(InfluxDBUtils.TAGS, "success");
        runner.setProperty(InfluxDBUtils.FIELDS, "orderTime,healthy");

        recordReader.addSchemaField("success", RecordFieldType.STRING);
        recordReader.addSchemaField("orderTime", RecordFieldType.DATE);
        recordReader.addSchemaField("healthy", RecordFieldType.BOOLEAN);

        recordReader.addRecord(Boolean.TRUE, new Date(), Boolean.TRUE);

        runner.enqueue("");
        runner.run();

        // success save first record
        runner.assertTransferCount(PutInfluxDatabaseRecord.REL_SUCCESS, 1);

        // create second record
        recordReader = new MockRecordParser();
        runner.addControllerService("recordReader", recordReader);
        runner.enableControllerService(recordReader);

        // wrong type
        recordReader.addSchemaField("success", RecordFieldType.STRING);
        recordReader.addSchemaField("orderTime", RecordFieldType.BOOLEAN);
        recordReader.addSchemaField("healthy", RecordFieldType.DATE);

        recordReader.addRecord(Boolean.TRUE, Boolean.TRUE, new Date());

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(PutInfluxDatabaseRecord.REL_FAILURE, 1);
    }

    @Test
    public void saveInfluxLineProtocol() throws InitializationException {

        String data = "weather,location=us-midwest temperature=82 1465839830100400200";

        InfluxLineProtocolReader readerFactory = new InfluxLineProtocolReader();

        runner.addControllerService("inline-reader", readerFactory);
        runner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        runner.enableControllerService(readerFactory);

        runner.setProperty(PutInfluxDatabaseRecord.RECORD_READER_FACTORY, "inline-reader");
        runner.setProperty(InfluxDBUtils.TAGS, "tags");
        runner.setProperty(InfluxDBUtils.FIELDS, "fields");
        runner.setProperty(InfluxDBUtils.TIMESTAMP_FIELD, "timestamp");

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        FluxRecord record = tables.get(0).getRecords().get(0);

        Assert.assertEquals("testRecordMeasurement", record.getMeasurement());
        Assert.assertEquals("us-midwest", record.getValueByKey("location"));
        Assert.assertEquals(82D, record.getValueByKey("temperature"));

        Assert.assertEquals(Instant.ofEpochSecond(0, 1465839830100400200L), record.getTime());
    }
}