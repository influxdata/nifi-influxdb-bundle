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
package org.influxdata.nifi.serialization;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.influxdata.nifi.processors.MapperOptions;
import org.influxdata.nifi.util.InfluxDBUtils;
import org.influxdata.nifi.util.PropertyValueUtils;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * @author Jakub Bednar (bednar@github) (29/05/2019 09:07)
 */
@Tags({"influxdb", "measurement", "timeseries", "line protocol", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@CapabilityDescription("Writes the contents of a RecordSet as Line Protocol. The configured writer "
        + "is able to make Line Protocol by the Expression Language that reference each of the fields "
        + "that are available in a Record. Each record in the RecordSet will be separated "
        + "by a single newline character. The Record Schema is read from the incoming FlowFile.")
public class InfluxLineProtocolRecordSetWriter extends AbstractControllerService implements RecordSetWriterFactory {

    private static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("influxdb-character-set")
            .displayName("Character Set")
            .description("The Character Encoding that is used to encode/decode the Line Protocol")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .required(true)
            .build();

    private String charSet;
    private MapperOptions mapperOptions;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());

        properties.add(InfluxDBUtils.MEASUREMENT);
        properties.add(InfluxDBUtils.TAGS);
        properties.add(InfluxDBUtils.MISSING_TAG_BEHAVIOR);
        properties.add(InfluxDBUtils.FIELDS);
        properties.add(InfluxDBUtils.MISSING_FIELD_BEHAVIOR);
        properties.add(InfluxDBUtils.TIMESTAMP_FIELD);
        properties.add(InfluxDBUtils.TIMESTAMP_PRECISION);
        properties.add(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR);
        properties.add(InfluxDBUtils.NULL_VALUE_BEHAVIOR);
        properties.add(CHARSET);

        return properties;
    }

    @OnEnabled
    public void storeMapperOptions(final ConfigurationContext context) throws PropertyValueUtils.IllegalConfigurationException {
        this.charSet = context.getProperty(CHARSET).getValue();
        this.mapperOptions = PropertyValueUtils.getMapperOptions(context, null);
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
        return readSchema;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog componentLog, final RecordSchema recordSchema, final OutputStream outputStream) {
        return new WriteInfluxLineProtocolResult(outputStream, recordSchema, componentLog, mapperOptions, charSet);
    }
}