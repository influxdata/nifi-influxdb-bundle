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

import java.lang.reflect.Type;
import java.util.List;

import org.influxdata.nifi.util.InfluxDBUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import com.google.gson.reflect.TypeToken;
import org.apache.nifi.util.TestRunner;
import org.junit.After;

/**
 * Base integration test class for InfluxDB processors
 */
public abstract class AbstractITInfluxDB {
    protected TestRunner runner;
    protected InfluxDB influxDB;
    protected String dbName = "test";
    protected String dbUrl = "http://localhost:8086";
    protected String user = "admin";
    protected String password = "admin";
    protected static final String DEFAULT_RETENTION_POLICY = "autogen";

    protected Type QueryResultListType = new TypeToken<List<QueryResult>>(){}.getType();

    protected void initInfluxDB() throws Exception {
        influxDB = InfluxDBUtils.makeConnectionV1(dbUrl, user, password, 10, null);
        influxDB.query(new Query("CREATE database " + dbName, dbName));
        int max = 10;
        while (!databaseExists(dbName) && (max-- < 0)) {
            Thread.sleep(5);
        }
        if ( ! databaseExists(dbName) ) {
            throw new Exception("unable to create database " + dbName);
        }
    }

    protected void cleanUpDatabase() throws InterruptedException {
        if ( databaseExists(dbName) ) {
            QueryResult result = influxDB.query(new Query("DROP measurement water", dbName));
            checkError(result);
            result = influxDB.query(new Query("DROP measurement testm", dbName));
            checkError(result);
            result = influxDB.query(new Query("DROP measurement chunkedQueryTest", dbName));
            checkError(result);
            result = influxDB.query(new Query("DROP measurement testRecordMeasurement", dbName));
            checkError(result);
            result = influxDB.query(new Query("DROP database " + dbName, dbName));
            Thread.sleep(1000);
        }
    }

    protected void checkError(QueryResult result) {
        if ( result.hasError() ) {
            throw new IllegalStateException("Error while dropping measurements " + result.getError());
        }
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
        if ( influxDB != null ) {
            cleanUpDatabase();
            influxDB.close();
        }
    }

    protected void initializeRunner() {
        runner.setProperty(ExecuteInfluxDatabaseQuery.DB_NAME, dbName);
        runner.setProperty(ExecuteInfluxDatabaseQuery.USERNAME, user);
        runner.setProperty(ExecuteInfluxDatabaseQuery.PASSWORD, password);
        runner.setProperty(ExecuteInfluxDatabaseQuery.INFLUX_DB_URL, dbUrl);
        runner.setProperty(ExecuteInfluxDatabaseQuery.CHARSET, "UTF-8");
        runner.assertValid();
    }

    private boolean databaseExists(String dbName) {
        QueryResult result = influxDB.query(new Query("SHOW databases", dbName));
        List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();

        return databaseNames.stream().anyMatch(name -> name.get(0).toString().equals(dbName));
    }
}