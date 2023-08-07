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

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.processors.standard.ConvertRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdata.nifi.util.InfluxDBUtils;
import org.junit.jupiter.api.Test;

/**
 * @author Jakub Bednar (bednar@github) (30/05/2019 11:29)
 */
public class TestInfluxLineProtocolRecordSetWriter {

    @Test
    public void processIncomingCSVToLineProtocol() throws InitializationException {

        final String content = "location,amount,production\n"
                + "west,10,true\n"
                + "north,20,false\n"
                + "south,30,true\n";

        ConvertRecord processor = new ConvertRecord();
        TestRunner convertRunner = TestRunners.newTestRunner(processor);

        final CSVReader csvReader = new CSVReader();
        convertRunner.addControllerService("record-reader", csvReader);
        convertRunner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        convertRunner.setProperty(csvReader, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_MINIMAL.getValue());
        convertRunner.setProperty(csvReader, CSVUtils.TRAILING_DELIMITER, "false");
        convertRunner.enableControllerService(csvReader);

        InfluxLineProtocolRecordSetWriter writerFactory = new InfluxLineProtocolRecordSetWriter();
        convertRunner.addControllerService("record-writer", writerFactory);
        convertRunner.setProperty(writerFactory, InfluxDBUtils.MEASUREMENT, "air");
        convertRunner.setProperty(writerFactory, InfluxDBUtils.TAGS, "location,production");
        convertRunner.setProperty(writerFactory, InfluxDBUtils.FIELDS, "amount");
        convertRunner.enableControllerService(writerFactory);

        convertRunner.setProperty("record-reader", "record-reader");
        convertRunner.setProperty("record-writer", "record-writer");


        convertRunner.enqueue(content);
        convertRunner.run();
        convertRunner.assertAllFlowFilesTransferred("success", 1);
        convertRunner.getFlowFilesForRelationship("success").get(0).assertContentEquals(
                "air,location=west,production=true amount=10i\n"
                + "air,location=north,production=false amount=20i\n"
                + "air,location=south,production=true amount=30i\n");
    }
}
