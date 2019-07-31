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
import java.util.Arrays;
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
 * @author Jakub Bednar (bednar@github) (19/07/2019 10:01)
 */
@CapabilityDescription("Creates FlowFiles from records in InfluxDB 2.0 loaded by a user-specified Flux query.")
@Tags({"influxdb", "measurement", "get", "fetch", "timeseries", "2.0", "flux"})
@WritesAttributes({
        @WritesAttribute(
                attribute = AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE,
                description = "InfluxDB error message"),
})
@EventDriven
public class GetInfluxDatabase_2 extends AbstractGetInfluxDatabase_2 {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    private static final Set<Relationship> RELATIONSHIPS;

    static {

        RECORDS_PER_FLOWFILE = new PropertyDescriptor.Builder()
                .name("influxdb-records-per-flowfile")
                .displayName("Results Per FlowFile")
                .description("How many records to put into a FlowFile at once. The whole body will be treated as a CSV file.")
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

    @Override
    protected Dialect prepareDialect(final ProcessContext context) {

        boolean dialectHeader = context.getProperty(DIALECT_HEADER).asBoolean();
        String dialectDelimiter = context.getProperty(DIALECT_DELIMITER).getValue();
        String dialectCommentPrefix = context.getProperty(DIALECT_COMMENT_PREFIX).getValue();
        Dialect.DateTimeFormatEnum dialectTimeFormat = Dialect.DateTimeFormatEnum
                .fromValue(context.getProperty(DIALECT_DATE_TIME_FORMAT).getValue());

        List<Dialect.AnnotationsEnum> dialectAnnotations = new ArrayList<>();
        String dialectAnnotationsValue = context.getProperty(DIALECT_ANNOTATIONS).getValue();
        if (dialectAnnotationsValue != null && !dialectAnnotationsValue.isEmpty()) {
            Arrays.stream(dialectAnnotationsValue.split(","))
                    .filter(annotation -> annotation != null && !annotation.trim().isEmpty())
                    .map(String::trim)
                    .forEach(annotation -> dialectAnnotations.add(Dialect.AnnotationsEnum.fromValue(annotation.trim())));
        }

        return new Dialect()
                .header(dialectHeader)
                .delimiter(dialectDelimiter)
                .commentPrefix(dialectCommentPrefix)
                .dateTimeFormat(dialectTimeFormat)
                .annotations(dialectAnnotations);
    }
}