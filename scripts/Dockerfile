ARG NIFI_IMAGE=apache/nifi:1.8.0

FROM $NIFI_IMAGE

USER nifi

COPY --chown=nifi:nifi ./nifi-influx-database-nar/target/nifi-influx-database-nar-*.nar lib
COPY --chown=nifi:nifi ./scripts/flow.xml.gz conf

CMD ${NIFI_BASE_DIR}/scripts/start.sh
