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

import org.junit.Test;

/**
 * @author Jakub Bednar (24/07/2019 08:17)
 */
public class TestGetInfluxDatabaseRecordSettings_2 extends AbstractTestGetInfluxDatabaseRecord_2 {

    @Test
    public void defaultSettingsValid() {

        runner.assertValid();
    }

    @Test
    public void testEmptyDBService() {
        runner.setProperty(GetInfluxDatabaseRecord_2.INFLUX_DB_SERVICE, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyWriterFactory() {
        runner.setProperty(GetInfluxDatabaseRecord_2.WRITER_FACTORY, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyOrg() {
        runner.setProperty(GetInfluxDatabaseRecord_2.ORG, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyQuery() {
        runner.setProperty(GetInfluxDatabaseRecord_2.ORG, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyDateTimeFormat() {
        runner.setProperty(GetInfluxDatabaseRecord_2.DIALECT_DATE_TIME_FORMAT, "");
        runner.assertNotValid();
    }

    @Test
    public void testNotSupportedDateTimeFormatValue() {
        runner.setProperty(GetInfluxDatabaseRecord_2.DIALECT_DATE_TIME_FORMAT, "not-supported");
        runner.assertNotValid();
    }

    @Test
    public void testRecordsPerFlowfile() {
        runner.setProperty(GetInfluxDatabaseRecord_2.RECORDS_PER_FLOWFILE, "5");
        runner.assertValid();
    }

    @Test
    public void testNegativeRecordsPerFlowfile() {
        runner.setProperty(GetInfluxDatabaseRecord_2.RECORDS_PER_FLOWFILE, "-1");
        runner.assertNotValid();
    }
}
