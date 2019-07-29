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

import java.util.List;

import org.influxdata.client.domain.Dialect;
import org.influxdata.client.domain.Query;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.REL_SUCCESS;

/**
 * @author Jakub Bednar (23/07/2019 09:22)
 */
public class TestGetInfluxDatabase_2 extends AbstractTestGetInfluxDatabase_2 {

    @Test
    public void queryValue() {

        runner.enqueue("");
        runner.run();

        Query query = new Query()
                .query("from(bucket:\"my-bucket\") |> range(start: 0) |> last()")
                .dialect(new Dialect()
                        .header(false)
                        .delimiter(",")
                        .commentPrefix("#")
                        .dateTimeFormat(Dialect.DateTimeFormatEnum.RFC3339));

        Mockito.verify(mockQueryApi, Mockito.only()).queryRaw(Mockito.eq(query), Mockito.eq("my-org"), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void queryValueWithCustomDialect() {

        runner.setProperty(GetInfluxDatabase_2.DIALECT_HEADER, "true");
        runner.setProperty(GetInfluxDatabase_2.DIALECT_DELIMITER, "|");
        runner.setProperty(GetInfluxDatabase_2.DIALECT_COMMENT_PREFIX, "§");
        runner.setProperty(GetInfluxDatabase_2.DIALECT_ANNOTATIONS, "datatype, group");
        runner.setProperty(GetInfluxDatabase_2.DIALECT_DATE_TIME_FORMAT, "RFC3339Nano");

        runner.enqueue("");
        runner.run();

        Query query = new Query()
                .query("from(bucket:\"my-bucket\") |> range(start: 0) |> last()")
                .dialect(new Dialect()
                        .header(true)
                        .delimiter("|")
                        .commentPrefix("§")
                        .addAnnotationsItem(Dialect.AnnotationsEnum.DATATYPE)
                        .addAnnotationsItem(Dialect.AnnotationsEnum.GROUP)
                        .dateTimeFormat(Dialect.DateTimeFormatEnum.RFC3339NANO));

        Mockito.verify(mockQueryApi, Mockito.only()).queryRaw(Mockito.eq(query), Mockito.eq("my-org"), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void success() {

        prepareData();

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(REL_SUCCESS).get(0).assertContentEquals(String.join("\n", queryOnResponseRecords));
    }

    @Test
    public void successMoreFiles() {

        prepareData();

        runner.setProperty(GetInfluxDatabase_2.RECORDS_PER_FLOWFILE, "2");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 4);
        runner.getFlowFilesForRelationship(REL_SUCCESS).get(0).assertContentEquals("");
        runner.getFlowFilesForRelationship(REL_SUCCESS).get(1).assertContentEquals(String.join("\n", queryOnResponseRecords.subList(0, 2)));
        runner.getFlowFilesForRelationship(REL_SUCCESS).get(2).assertContentEquals(String.join("\n", queryOnResponseRecords.subList(2, 4)));
        runner.getFlowFilesForRelationship(REL_SUCCESS).get(3).assertContentEquals(String.join("\n", queryOnResponseRecords.subList(4, 6)));
    }

    @Test
    public void containsProvenanceReport() {

        prepareData();

        runner.enqueue("");
        runner.run();

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();

        Assert.assertEquals(1, events.size());

        ProvenanceEventRecord record = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, record.getEventType());
        Assert.assertEquals(processor.getIdentifier(), record.getComponentId());
        Assert.assertEquals("http://localhost:8086", record.getTransitUri());
    }

    private void prepareData() {
        queryOnResponseRecords.add(",,0,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,A,west,121,11,test");
        queryOnResponseRecords.add(",,1,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,free,mem,B,west,484,22,test");
        queryOnResponseRecords.add(",,2,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,usage_system,cpu,A,west,1444,38,test");
        queryOnResponseRecords.add(",,3,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,user_usage,cpu,A,west,2401,49,test");
        queryOnResponseRecords.add(",,4,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,user_usage,cpu,C,west,2401,49,test");
        queryOnResponseRecords.add(",,5,1677-09-21T00:12:43.145224192Z,2018-07-16T11:21:02.547596934Z,user_usage,cpu,D,west,2401,49,test");
    }
}
