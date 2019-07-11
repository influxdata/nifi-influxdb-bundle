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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.CHARSET;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_FAILURE;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_RETRY;
import static org.influxdata.nifi.processors.PutInfluxDatabase.REL_SUCCESS;

/**
 * @author Jakub Bednar (bednar@github) (11/07/2019 07:44)
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"influxdb", "measurement", "insert", "write", "put", "timeseries", "2.0"})
@CapabilityDescription("Processor to write the content of a FlowFile in 'line protocol'. Please check details of the 'line protocol' in InfluxDB 2.0 documentation (https://www.influxdb.com/). "
        + " The flow file can contain single measurement point or multiple measurement points separated by line separator."
        + " The timestamp precision is defined by Timestamp property. If you do not specify precision then the InfluxDB assumes that timestamps are in nanoseconds.")
@WritesAttributes({
        @WritesAttribute(attribute = AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE, description = "InfluxDB error message"),
})
public class PutInfluxDatabase_2 extends AbstractInfluxDatabaseProcessor_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {

        final Set<Relationship> relationships = new LinkedHashSet<>();

        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);

        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        propertyDescriptors.add(INFLUX_DB_SERVICE);
        propertyDescriptors.add(BUCKET);
        propertyDescriptors.add(ORG);
        propertyDescriptors.add(TIMESTAMP_PRECISION);

        propertyDescriptors.add(ENABLE_GZIP);
        propertyDescriptors.add(LOG_LEVEL);

        propertyDescriptors.add(CHARSET);
        propertyDescriptors.add(MAX_RECORDS_SIZE);

        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(propertyDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    }
}