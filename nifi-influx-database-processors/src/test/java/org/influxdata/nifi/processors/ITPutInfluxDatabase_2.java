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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import org.influxdata.client.InfluxDBClient;
import org.influxdata.client.InfluxDBClientFactory;
import org.influxdata.client.InfluxDBClientOptions;
import org.influxdata.client.QueryApi;
import org.influxdata.client.domain.Authorization;
import org.influxdata.client.domain.Bucket;
import org.influxdata.client.domain.BucketRetentionRules;
import org.influxdata.client.domain.Organization;
import org.influxdata.client.domain.Permission;
import org.influxdata.client.domain.PermissionResource;
import org.influxdata.client.domain.WritePrecision;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;
import org.influxdata.nifi.services.InfluxDatabaseService_2;
import org.influxdata.nifi.services.StandardInfluxDatabaseService_2;
import org.influxdata.query.FluxTable;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_ACCESS_TOKEN;
import static org.influxdata.nifi.services.InfluxDatabaseService_2.INFLUX_DB_URL;
import static org.junit.Assert.assertEquals;

/**
 * @author Jakub Bednar (bednar@github) (15/07/2019 09:05)
 */
public class ITPutInfluxDatabase_2 {

    private TestRunner runner;

    private String bucketName;

    private QueryApi queryApi;
    private InfluxDBClient influxDBClient;

    @Before
    public void setUp() throws Exception {

        String url = "http://localhost:9999";

        influxDBClient = InfluxDBClientFactory.create(url, "my-token".toCharArray());

        Organization organization = influxDBClient.getOrganizationsApi().findOrganizations().stream()
                .filter(it -> it.getName().equals("my-org"))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        bucketName = "nifi-bucket-" + System.currentTimeMillis();

        Bucket bucket = influxDBClient.getBucketsApi()
                .createBucket(bucketName, new BucketRetentionRules().everySeconds(3600), organization);

        PermissionResource resource = new PermissionResource();
        resource.setId(bucket.getId());
        resource.setOrgID(organization.getId());
        resource.setType(PermissionResource.TypeEnum.BUCKETS);

        //
        // Add Permissions to read and write to the Bucket
        //
        Permission readBucket = new Permission();
        readBucket.setResource(resource);
        readBucket.setAction(Permission.ActionEnum.READ);

        Permission writeBucket = new Permission();
        writeBucket.setResource(resource);
        writeBucket.setAction(Permission.ActionEnum.WRITE);

        Authorization authorization = influxDBClient.getAuthorizationsApi()
                .createAuthorization(organization, Arrays.asList(readBucket, writeBucket));

        String token = authorization.getToken();

        influxDBClient.close();
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .authenticateToken(token.toCharArray())
                .org(organization.getId())
                .build();
        influxDBClient = InfluxDBClientFactory.create(options);
        queryApi = influxDBClient.getQueryApi();

        PutInfluxDatabase_2 putInfluxDatabase_2 = new PutInfluxDatabase_2();
        runner = TestRunners.newTestRunner(putInfluxDatabase_2);
        runner.setProperty(PutInfluxDatabase_2.INFLUX_DB_SERVICE, "influxdb-service");
        runner.setProperty(PutInfluxDatabase_2.BUCKET, bucketName);
        runner.setProperty(PutInfluxDatabase_2.ORG, "my-org");

        InfluxDatabaseService_2 influxDatabaseService = Mockito.spy(new StandardInfluxDatabaseService_2());

        runner.addControllerService("influxdb-service", influxDatabaseService);
        runner.setProperty(influxDatabaseService, INFLUX_DB_URL, url);
        runner.setProperty(influxDatabaseService, INFLUX_DB_ACCESS_TOKEN, "my-token");
        runner.enableControllerService(influxDatabaseService);
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
        influxDBClient.close();
    }

    @Test
    public void testValid() {
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0,1), tables.get(0).getRecords().get(0).getTime());
    }
    
    @Test
    public void testBadFormat() {
        runner.enqueue("water,country=US,city=newark".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_FAILURE);

        assertEquals(1, flowFiles.size());
        assertEquals("unable to parse points: unable to parse 'water,country=US,city=newark': missing fields",
                flowFiles.get(0).getAttribute(AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void testPrecisionNanosecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.NS.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0,1), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionMicrosecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.US.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MICROS), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionMillisecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.MS.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.MILLIS), tables.get(0).getRecords().get(0).getTime());
    }

    @Test
    public void testPrecisionSecond() {
        runner.setProperty(PutInfluxDatabase_2.TIMESTAMP_PRECISION, WritePrecision.S.name());
        runner.enqueue("water,country=US,city=newark humidity=0.6 1".getBytes());
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(AbstractInfluxDatabaseProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractInfluxDatabaseProcessor.REL_SUCCESS);

        assertEquals(1, flowFiles.size());

        List<FluxTable> tables = queryApi.query("from(bucket:\"" + bucketName + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()");

        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(1, tables.get(0).getRecords().size());
        Assert.assertEquals("water", tables.get(0).getRecords().get(0).getMeasurement());
        Assert.assertEquals("US", tables.get(0).getRecords().get(0).getValueByKey("country"));
        Assert.assertEquals("newark", tables.get(0).getRecords().get(0).getValueByKey("city"));
        Assert.assertEquals(0.6D, tables.get(0).getRecords().get(0).getValue());
        Assert.assertEquals("humidity", tables.get(0).getRecords().get(0).getField());
        Assert.assertEquals(Instant.ofEpochSecond(0).plus(1, ChronoUnit.SECONDS), tables.get(0).getRecords().get(0).getTime());
    }
}