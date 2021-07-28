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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.influxdb.client.domain.WritePrecision;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLRecordSetWriter;
import org.assertj.core.api.Assertions;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.Diff;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_URL;

/**
 * @author Jakub Bednar (25/07/2019 07:10)
 */
public class ITGetInfluxDatabaseRecord_2 extends AbstractITInfluxDB_2 {

    private AvroRecordSetWriter recordWriter;

    @Before
    public void setUp() throws Exception {

        init();

        GetInfluxDatabaseRecord_2 putInfluxDatabase_2 = new GetInfluxDatabaseRecord_2();
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
		runner.setValidateExpressionUsage(false);
        runner.setProperty(GetInfluxDatabaseRecord_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(GetInfluxDatabaseRecord_2.ORG, organization.getId());
        runner.setProperty(GetInfluxDatabaseRecord_2.QUERY, "from(bucket:\"" + bucketName + "\") |> range(start: 0)");
        runner.setProperty(GetInfluxDatabaseRecord_2.LOG_LEVEL, "BODY");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_URL, INFLUX_DB_2);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);

        recordWriter = new AvroRecordSetWriter();
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(recordWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(GetInfluxDatabaseRecord_2.WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.assertValid();
    }

    @Test
    public void oneRow() throws IOException {

        Timestamp from = Timestamp.from(Instant.now());

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabaseRecord_2.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(0);

        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(
                new ByteArrayInputStream(flowFile.toByteArray()),
                new GenericDatumReader<>());
        GenericData.setStringType(dataFileStream.getSchema(), GenericData.StringType.String);

        GenericRecord avroRecord = dataFileStream.next();
        Assert.assertEquals(10, avroRecord.getSchema().getFields().size());
        Assert.assertEquals("_result", ((Utf8) avroRecord.get("result")).toString());
        Assert.assertEquals(0L, avroRecord.get("table"));
        Assert.assertEquals(0L, avroRecord.get("_start"));
        Assert.assertTrue((long) avroRecord.get("_stop") > from.getTime());
        Assert.assertEquals(0L, avroRecord.get("_time"));
        Assert.assertEquals("humidity", ((Utf8) avroRecord.get("_field")).toString());
        Assert.assertEquals("water", ((Utf8) avroRecord.get("_measurement")).toString());
        Assert.assertEquals("newark", ((Utf8) avroRecord.get("city")).toString());
        Assert.assertEquals("US", ((Utf8) avroRecord.get("country")).toString());
        Assert.assertEquals(0.6D, avroRecord.get("_value"));
    }

    @Test
    public void moreRows() throws IOException {

        influxDBClient.getWriteApiBlocking().writeRecords(WritePrecision.NS, Arrays.asList(
                "water,country=US,city=newark humidity=0.1 1000000",
                "water,country=US,city=newark humidity=0.2 2000000",
                "water,country=US,city=newark humidity=0.3 3000000",
                "water,country=US,city=newark humidity=0.4 4000000",
                "water,country=US,city=newark humidity=0.5 5000000",
                "water,country=US,city=newark humidity=0.6 6000000"
        ));

        runner.setProperty(GetInfluxDatabaseRecord_2.RECORDS_PER_FLOWFILE, "2");
        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabaseRecord_2.REL_SUCCESS, 3);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(0);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(
                new ByteArrayInputStream(flowFile.toByteArray()),
                new GenericDatumReader<>());
        GenericData.setStringType(dataFileStream.getSchema(), GenericData.StringType.String);
        List<GenericRecord> genericRecords = Lists.newArrayList(dataFileStream.iterator());
        Assert.assertEquals(2, genericRecords.size());
        Assert.assertEquals(0.1D, genericRecords.get(0).get("_value"));
        Assert.assertEquals(1L, genericRecords.get(0).get("_time"));
        Assert.assertEquals(0.2D, genericRecords.get(1).get("_value"));
        Assert.assertEquals(2L, genericRecords.get(1).get("_time"));

        flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(1);
        dataFileStream = new DataFileStream<>(
                new ByteArrayInputStream(flowFile.toByteArray()),
                new GenericDatumReader<>());
        GenericData.setStringType(dataFileStream.getSchema(), GenericData.StringType.String);
        genericRecords = Lists.newArrayList(dataFileStream.iterator());
        Assert.assertEquals(2, genericRecords.size());
        Assert.assertEquals(0.3D, genericRecords.get(0).get("_value"));
        Assert.assertEquals(3L, genericRecords.get(0).get("_time"));
        Assert.assertEquals(0.4D, genericRecords.get(1).get("_value"));
        Assert.assertEquals(4L, genericRecords.get(1).get("_time"));

        flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(2);
        dataFileStream = new DataFileStream<>(
                new ByteArrayInputStream(flowFile.toByteArray()),
                new GenericDatumReader<>());
        GenericData.setStringType(dataFileStream.getSchema(), GenericData.StringType.String);
        genericRecords = Lists.newArrayList(dataFileStream.iterator());
        Assert.assertEquals(2, genericRecords.size());
        Assert.assertEquals(0.5D, genericRecords.get(0).get("_value"));
        Assert.assertEquals(5L, genericRecords.get(0).get("_time"));
        Assert.assertEquals(0.6D, genericRecords.get(1).get("_value"));
        Assert.assertEquals(6L, genericRecords.get(1).get("_time"));
    }

    @Test
    public void writeToJson() throws InitializationException {

        runner.removeControllerService(recordWriter);

        JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.enableControllerService(writer);

        Timestamp from = Timestamp.from(Instant.now());

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabaseRecord_2.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(0);

        String json = new String(runner.getContentAsByteArray(flowFile));
		JsonObject parsedJson = (new JsonParser().parse(json).getAsJsonArray()).get(0).getAsJsonObject();

        Assert.assertNotNull(parsedJson);
        Assert.assertEquals("_result", parsedJson.get("result").getAsString());
        Assert.assertEquals(0, parsedJson.get("table").getAsInt());
        Assert.assertEquals(0, parsedJson.get("_start").getAsInt());
        Assert.assertEquals(0, parsedJson.get("_time").getAsInt());
        Assert.assertTrue(parsedJson.get("_stop").getAsLong() > from.getTime());
        Assert.assertEquals("humidity", parsedJson.get("_field").getAsString());
        Assert.assertEquals("newark", parsedJson.get("city").getAsString());
        Assert.assertEquals("US", parsedJson.get("country").getAsString());
        Assert.assertEquals((Object) 0.6D, parsedJson.get("_value").getAsDouble());
    }

    @Test
    public void writeToCSV() throws InitializationException {
        runner.removeControllerService(recordWriter);

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("writer", csvWriter);
        runner.setProperty(csvWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(csvWriter, CSVUtils.INCLUDE_HEADER_LINE, "false");
        runner.enableControllerService(csvWriter);

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabaseRecord_2.REL_SUCCESS, 1);
        String data = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabase_2.REL_SUCCESS).get(0))).trim();
        Assert.assertTrue(data.startsWith("_result,0,0,"));
        Assert.assertTrue(data.endsWith(",0,0.6,humidity,water,newark,US"));
    }

    @Test
    public void writeToXML() throws InitializationException {
        runner.removeControllerService(recordWriter);

        final XMLRecordSetWriter xmlRecordSetWriter = new XMLRecordSetWriter();
        runner.addControllerService("writer", xmlRecordSetWriter);
        runner.setProperty(xmlRecordSetWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(xmlRecordSetWriter, XMLRecordSetWriter.ROOT_TAG_NAME, "root");
        runner.setProperty(xmlRecordSetWriter, XMLRecordSetWriter.RECORD_TAG_NAME, "record");
        runner.enableControllerService(xmlRecordSetWriter);
        runner.assertValid();

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.run(1);

        runner.assertAllFlowFilesTransferred(GetInfluxDatabaseRecord_2.REL_SUCCESS, 1);

        String expected ="<?xml version=\"1.0\" ?>"
                + "<root>"
                + "<record>"
                + "<result>_result</result>"
                + "<table>0</table>"
                + "<_start>0</_start>"
                + "<_stop>1564039172097</_stop>"
                + "<_time>0</_time>"
                + "<_value>0.6</_value>"
                + "<_field>humidity</_field>"
                + "<_measurement>water</_measurement>"
                + "<city>newark</city>"
                + "<country>US</country>"
                + "</record>"
                + "</root>";
        String actual = new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_SUCCESS).get(0)));

        Diff myDiff = DiffBuilder.compare(expected).withTest(actual)
                .withNodeFilter(node -> !"_stop".equals(node.getNodeName()))
                .checkForSimilar().build();

        Assert.assertFalse(myDiff.toString(), myDiff.hasDifferences());
    }

    @Test
    public void invalidFlux() {

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.setProperty(GetInfluxDatabaseRecord_2.QUERY, "from(bucket:\"" + bucketName + "\") |> rangex(start: 0)");

        runner.run(1);

        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_SUCCESS, 0);
        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_RETRY, 0);
        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetInfluxDatabaseRecord_2.REL_FAILURE).get(0);
        Assertions.assertThat(flowFile.getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE)).contains("undefined identifier rangex");
    }

    @Test
    public void wrongParameters() {

        influxDBClient.getWriteApiBlocking().writeRecord(WritePrecision.NS, "water,country=US,city=newark humidity=0.6 1");

        runner.setProperty(GetInfluxDatabaseRecord_2.ORG, "not-exists");

        runner.run(1);

        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_SUCCESS, 0);
        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_RETRY, 0);
        runner.assertTransferCount(GetInfluxDatabaseRecord_2.REL_FAILURE, 1);
    }
}
