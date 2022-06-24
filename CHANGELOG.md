## v1.20.0 [unreleased]

### Others
1. [#73](https://github.com/influxdata/nifi-influxdb-bundle/pull/73): Update to Apache NiFi 1.16.3

## v1.19.0 [2022-06-24]

### Others
1. [#72](https://github.com/influxdata/nifi-influxdb-bundle/pull/72): Update to Apache NiFi 1.16.2

## v1.18.0 [2022-05-20]

### Others
1. [#70](https://github.com/influxdata/nifi-influxdb-bundle/pull/70): Update to Apache NiFi 1.16.1

## v1.17.0 [2022-04-21]

### Others
1. [#68](https://github.com/influxdata/nifi-influxdb-bundle/pull/68): Update to Apache NiFi 1.16.0

## v1.16.0 [2022-04-19]

### Features
1. [#67](https://github.com/influxdata/nifi-influxdb-bundle/pull/67): Add `Retry-After` header value when a FlowFile is transferred to `Retry` output

### Others
1. [#65](https://github.com/influxdata/nifi-influxdb-bundle/pull/65): Update to Apache NiFi 1.15.3

### CI
1. [#66](https://github.com/influxdata/nifi-influxdb-bundle/pull/66): Use new Codecov uploader for reporting code coverage

## v1.15.0 [2022-01-20]

### Others
1. [#64](https://github.com/influxdata/nifi-influxdb-bundle/pull/64): Update to Apache NiFi 1.15.2

## v1.14.0 [2021-11-26]

### Others
1. [#62](https://github.com/influxdata/nifi-influxdb-bundle/pull/62): Update to Apache NiFi 1.15.0

## v1.13.0 [2021-08-20]

### Others
1. [#60](https://github.com/influxdata/nifi-influxdb-bundle/pull/60): Update to Apache NiFi 1.14.0

## v1.12.0 [2021-07-09]

### Bug Fixes
1. [#58](https://github.com/influxdata/nifi-influxdb-bundle/pull/58): Initialization SSL Context

## v1.11.0 [2021-06-04]

### Others
1. [#54](https://github.com/influxdata/nifi-influxdb-bundle/pull/54): Update `influxdb-client-java` to 2.2.0

## v1.10.0 [2021-04-01]

### Others
1. [#48](https://github.com/influxdata/nifi-influxdb-bundle/pull/48): Update to Apache NiFi 1.13.2

### Dependencies
1. [#49](https://github.com/influxdata/nifi-influxdb-bundle/pull/49): Update dependencies:
    - Guava to 30.1-jre

## v1.9.0 [2021-03-05]

### Others
1. [#46](https://github.com/influxdata/nifi-influxdb-bundle/pull/46): Update to Apache NiFi 1.13.0

### CI
1. [#44](https://github.com/influxdata/nifi-influxdb-bundle/pull/44): Update stable image to `influxdb:latest`
1. [#45](https://github.com/influxdata/nifi-influxdb-bundle/pull/45): Change CI service from travis-ci.org to CircleCI

## v1.8.0 [2020-10-02]

### API
1. [#36](https://github.com/influxdata/nifi-influxdb-bundle/pull/36): Default port changed from 9999 -> 8086

### Others
1. [#38](https://github.com/influxdata/nifi-influxdb-bundle/pull/38): Update NiFi to 1.12.1, InfluxDB v1 to 1.8, Chronograf to 1.7, influxdb-client-java to 1.11.0 and influxdb-java to 2.20

## v1.7.0 [2020-04-17]

### Others
1. [#33](https://github.com/influxdata/nifi-influxdb-bundle/pull/33): Update to Apache NiFi 1.11.4

## v1.6.0 [2020-03-13]

### Others
1. [#32](https://github.com/influxdata/nifi-influxdb-bundle/pull/32): Update to Apache NiFi 1.11.3

## v1.5.0 [2020-02-17]    

### Others
1. [#31](https://github.com/influxdata/nifi-influxdb-bundle/pull/31): Update to Apache NiFi 1.11.1

## v1.4 [2020-01-30]    

### Others
1. [#30](https://github.com/influxdata/nifi-influxdb-bundle/pull/30): Update to Apache NiFi 1.11.0

## v1.3 [2020-01-29]

### Others
1. [#27](https://github.com/influxdata/nifi-influxdb-bundle/pull/27): Update to Apache NiFi 1.10.0

## v1.2 [2019-12-06]

### Features

1. [#23](https://github.com/influxdata/nifi-influxdb-bundle/issues/23): Add support for InfluxDB v2.0

## v1.1 [2019-06-06]

### Features

1. [#19](https://github.com/influxdata/nifi-influxdb-bundle#influxlineprotocolrecordsetwriter): Allows writes the contents of a Record as Line Protocol
1. [#20](https://github.com/influxdata/nifi-influxdb-bundle#putinfluxdatabase): PutInfluxDatabase supports the custom timestamp precision

### Others
1. [#11](https://github.com/influxdata/nifi-influxdb-bundle/issues/11): Update to Apache NiFi 1.9.2
1. [#12](https://github.com/influxdata/nifi-influxdb-bundle/issues/13): Update to influxdb-java 2.15

## v1.0 [2019-02-21]

### Features
1. [PutInfluxDatabaseRecord](https://github.com/influxdata/nifi-influxdb-bundle#putinfluxdatabaserecord): Uses a specified RecordReader to write the content of a FlowFile into InfluxDB database.
1. [InfluxLineProtocolReader](https://github.com/influxdata/nifi-influxdb-bundle#influxlineprotocolreader): Parses the InfluxDB Line Protocol into NiFi Record
1. [InfluxDatabaseService](https://github.com/influxdata/nifi-influxdb-bundle#influxdatabaseservice): Allows sharing connection configuration among more NiFi processors

### Bug Fixes

### Documentation
1. [#1](https://github.com/influxdata/nifi-influxdb-bundle/issues/1): Integration with Code coverage service
1. [#2](https://github.com/influxdata/nifi-influxdb-bundle/issues/2): Continuous deployment to GitHub Releases
1. [#3](https://github.com/influxdata/nifi-influxdb-bundle/issues/3): README.md has to be updated with doc about processors, services and parsers
1. [#4](https://github.com/influxdata/nifi-influxdb-bundle/issues/4): Create CHANGELOG.md
1. [#6](https://github.com/influxdata/nifi-influxdb-bundle/issues/6): Create demo that will be used for end-to-end test
1. [#8](https://github.com/influxdata/nifi-influxdb-bundle/issues/8): Create end-to-end test

