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

import java.util.Arrays;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Authorization;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.Permission;
import com.influxdb.client.domain.PermissionResource;
import org.influxdata.nifi.util.InfluxDBUtils;

import org.apache.nifi.util.TestRunner;
import org.junit.After;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 11:30)
 */
abstract class AbstractITInfluxDB_2 {
    static final String INFLUX_DB_2 = "http://localhost:9999";

    protected TestRunner runner;

    protected String bucketName;
    protected QueryApi queryApi;
    protected InfluxDBClient influxDBClient;
    protected Organization organization;

    protected void init() {

        influxDBClient = InfluxDBUtils.makeConnectionV2(INFLUX_DB_2, "my-token", 10, null, null);

        organization = influxDBClient.getOrganizationsApi().findOrganizations().stream()
                .filter(it -> it.getName().equals("my-org"))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        bucketName = "nifi-bucket-" + System.currentTimeMillis();

        Bucket bucket = influxDBClient.getBucketsApi()
                .createBucket(bucketName, null, organization);

        PermissionResource resource = new PermissionResource();
        resource.setId(bucket.getId());
        resource.setOrgID(organization.getId());
        resource.setType(PermissionResource.TYPE_BUCKETS);

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
                .url(INFLUX_DB_2)
                .authenticateToken(token.toCharArray())
                .org(organization.getId())
                .bucket(bucket.getId())
                .build();
        influxDBClient = InfluxDBClientFactory.create(options);
        queryApi = influxDBClient.getQueryApi();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
        influxDBClient.close();
    }

}