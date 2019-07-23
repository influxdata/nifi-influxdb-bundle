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

import org.influxdata.nifi.processors.internal.AbstractGetInfluxDatabase;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;

/**
 * @author Jakub Bednar (bednar@github) (19/07/2019 10:01)
 */
@CapabilityDescription("Creates FlowFiles from records in InfluxDB 2.0 loaded by a user-specified Flux query.")
@Tags({"influxdb", "measurement", "get", "fetch", "record", "timeseries", "2.0", "flux"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = GetInfluxDatabase_2.INFLUXDB_ORG_NAME, description = "The organization where the results came from."),
})
@EventDriven
public class GetInfluxDatabase_2 extends AbstractGetInfluxDatabase {

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
        propertyDescriptors.add(ORG);

        propertyDescriptors.add(QUERY);

        //Dialect
        propertyDescriptors.add(DIALECT_HEADER);
        propertyDescriptors.add(DIALECT_DELIMITER);
        propertyDescriptors.add(DIALECT_ANNOTATIONS);
        propertyDescriptors.add(DIALECT_COMMENT_PREFIX);
        propertyDescriptors.add(DIALECT_DATE_TIME_FORMAT);

        propertyDescriptors.add(RECORDS_PER_FLOWFILE);
        propertyDescriptors.add(ENABLE_GZIP);
        propertyDescriptors.add(LOG_LEVEL);

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
}