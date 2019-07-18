package org.influxdata.nifi.processors;

import java.util.Arrays;

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

    protected void init() throws Exception {

        influxDBClient = InfluxDBClientFactory.create(INFLUX_DB_2, "my-token".toCharArray());

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
                .url(INFLUX_DB_2)
                .authenticateToken(token.toCharArray())
                .org(organization.getId())
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