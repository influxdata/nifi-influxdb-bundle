# Development

## Update Apache NiFi version

### Update sources to new version:

#### 1. `pom.xml`

    <parent>
        <groupId>org.apache.nifi</groupId>
        <artifactId>nifi-nar-bundles</artifactId>
        <version>1.18.0</version>
    </parent>

    <nifi.version>1.18.0</nifi.version>

- https://github.com/influxdata/nifi-influxdb-bundle/blob/8c36e469da2c4f6818e98044573e389ed5bfe50f/pom.xml#L27
- https://github.com/influxdata/nifi-influxdb-bundle/blob/8c36e469da2c4f6818e98044573e389ed5bfe50f/pom.xml#L106

#### 2. `scripts/Dockerfile`

    ARG NIFI_IMAGE=apache/nifi:1.18.0

- https://github.com/influxdata/nifi-influxdb-bundle/blob/8c36e469da2c4f6818e98044573e389ed5bfe50f/scripts/Dockerfile#L18

#### 3. `scripts/flow.xml`

Replace all occurrence of `<version>1.17.0</version>` to `<version>1.18.0</version>`.

#### 4. `scripts/nifi-restart.sh`

Replace `DEFAULT_NIFI_VERSION="1.17.0"` to `DEFAULT_NIFI_VERSION="1.18.0"`.

### Update compatibility matrix to new version:

    | [nifi-influx-database-nar-1.24.0-SNAPSHOT.nar](...) | 1.18.0 |

- https://github.com/influxdata/nifi-influxdb-bundle/blob/8c36e469da2c4f6818e98044573e389ed5bfe50f/README.md#L64

----
For more info see: https://github.com/influxdata/nifi-influxdb-bundle/pull/93.

