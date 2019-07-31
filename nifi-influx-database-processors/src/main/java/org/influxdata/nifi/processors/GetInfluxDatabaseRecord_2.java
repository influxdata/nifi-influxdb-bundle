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

import com.influxdb.client.domain.Dialect;
import org.influxdata.nifi.processors.internal.AbstractGetInfluxDatabase_2;
import org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * @author Jakub Bednar (23/07/2019 15:10)
 */
@CapabilityDescription("A record-based version of GetInfluxDatabase_2 that uses the Record writers to write the Flux result set.")
@Tags({"influxdb", "measurement", "get", "fetch", "record", "timeseries", "2.0", "flux"})
@WritesAttributes({
        @WritesAttribute(
                attribute = AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE,
                description = "InfluxDB error message"),
})
@EventDriven
public class GetInfluxDatabaseRecord_2 extends AbstractGetInfluxDatabase_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {

        RECORDS_PER_FLOWFILE = new PropertyDescriptor.Builder()
                .name("influxdb-records-per-flowfile")
                .displayName("Results Per FlowFile")
                .description("How many records to put into a FlowFile at once. The whole body will be treated as a set of Records.")
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();

        final Set<Relationship> relationships = new LinkedHashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        propertyDescriptors.add(INFLUX_DB_SERVICE);
        propertyDescriptors.add(WRITER_FACTORY);
        propertyDescriptors.add(ORG);

        propertyDescriptors.add(QUERY);

        //Dialect
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

    @Override
    protected Dialect prepareDialect(final ProcessContext context) {

        Dialect.DateTimeFormatEnum dialectTimeFormat = Dialect.DateTimeFormatEnum
                .fromValue(context.getProperty(DIALECT_DATE_TIME_FORMAT).getValue());

        return new Dialect().header(true)
                .delimiter(",")
                .commentPrefix("#")
                .dateTimeFormat(dialectTimeFormat)
                .addAnnotationsItem(Dialect.AnnotationsEnum.DATATYPE)
                .addAnnotationsItem(Dialect.AnnotationsEnum.GROUP)
                .addAnnotationsItem(Dialect.AnnotationsEnum.DEFAULT);
    }
}
