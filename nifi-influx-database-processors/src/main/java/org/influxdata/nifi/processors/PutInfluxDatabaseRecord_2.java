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

import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.MAX_RECORDS_SIZE;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.RECORD_READER_FACTORY;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.REL_FAILURE;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.REL_RETRY;
import static org.influxdata.nifi.processors.AbstractInfluxDatabaseProcessor.REL_SUCCESS;
import static org.influxdata.nifi.util.InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR;
import static org.influxdata.nifi.util.InfluxDBUtils.FIELDS;
import static org.influxdata.nifi.util.InfluxDBUtils.MEASUREMENT;
import static org.influxdata.nifi.util.InfluxDBUtils.MISSING_FIELD_BEHAVIOR;
import static org.influxdata.nifi.util.InfluxDBUtils.MISSING_TAG_BEHAVIOR;
import static org.influxdata.nifi.util.InfluxDBUtils.NULL_VALUE_BEHAVIOR;
import static org.influxdata.nifi.util.InfluxDBUtils.TAGS;
import static org.influxdata.nifi.util.InfluxDBUtils.TIMESTAMP_FIELD;

/**
 * @author Jakub Bednar (bednar@github) (16/07/2019 14:04)
 */
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"influxdb", "measurement", "insert", "write", "put", "record", "timeseries", "2.0"})
@CapabilityDescription("PutInfluxDatabaseRecord_2 processor uses a specified RecordReader to write the content of a FlowFile " +
        "into InfluxDB 2.0 database.")
@WritesAttributes({@WritesAttribute(
        attribute = AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE,
        description = "InfluxDB error message"),
})
public class PutInfluxDatabaseRecord_2 extends AbstractInfluxDatabaseProcessor_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {

        final Set<Relationship> relationships = new LinkedHashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(RECORD_READER_FACTORY);

        propertyDescriptors.add(INFLUX_DB_SERVICE);
        propertyDescriptors.add(BUCKET);
        propertyDescriptors.add(ORG);

        propertyDescriptors.add(MEASUREMENT);

        propertyDescriptors.add(TAGS);
        propertyDescriptors.add(MISSING_TAG_BEHAVIOR);

        propertyDescriptors.add(FIELDS);
        propertyDescriptors.add(MISSING_FIELD_BEHAVIOR);

        propertyDescriptors.add(TIMESTAMP_FIELD);
        propertyDescriptors.add(TIMESTAMP_PRECISION);

        propertyDescriptors.add(COMPLEX_FIELD_BEHAVIOR);
        propertyDescriptors.add(NULL_VALUE_BEHAVIOR);

        propertyDescriptors.add(ENABLE_GZIP);
        propertyDescriptors.add(LOG_LEVEL);

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