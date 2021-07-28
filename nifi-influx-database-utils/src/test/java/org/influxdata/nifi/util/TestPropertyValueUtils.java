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
package org.influxdata.nifi.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdata.nifi.processors.MapperOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPropertyValueUtils {
    private static final PropertyDescriptor TEST_ENUM_PROPERTY = new PropertyDescriptor.Builder()
            .name("enum-value")
            .displayName("Enum value")
            .defaultValue(PropertyEnum.ONE.name())
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues(Arrays.stream(PropertyEnum.values()).map(Enum::name).toArray(String[]::new))
            .sensitive(false)
            .build();

    private static final PropertyDescriptor TEST_LIST_PROPERTY = new PropertyDescriptor.Builder()
            .name("list-value")
            .displayName("List value")
            .defaultValue("")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    private enum PropertyEnum {

        ONE,

        TWO
    }

    private TestRunner testRunner;
    private ProcessContext processContext;

    @Before
    public void before() {

        testRunner = TestRunners.newTestRunner(TestProcessor.class);
        processContext = testRunner.getProcessContext();
    }

    @Test
    public void enumValue() {

        testRunner.setProperty(TEST_ENUM_PROPERTY, PropertyEnum.TWO.name());

        PropertyEnum propertyValue = PropertyValueUtils
                .getEnumValue(TEST_ENUM_PROPERTY, processContext, PropertyEnum.class, PropertyEnum.ONE);

        Assert.assertEquals(PropertyEnum.TWO, propertyValue);
    }

    @Test
    public void enumValueDefault() {

        PropertyEnum propertyValue = PropertyValueUtils
                .getEnumValue(TEST_ENUM_PROPERTY, processContext, PropertyEnum.class, PropertyEnum.ONE);

        Assert.assertEquals(PropertyEnum.ONE, propertyValue);
    }

    @Test
    public void enumValueNotExistConstant() {

        testRunner.setProperty(TEST_ENUM_PROPERTY, "not-exist-value");

        PropertyEnum propertyValue = PropertyValueUtils
                .getEnumValue(TEST_ENUM_PROPERTY, processContext, PropertyEnum.class, PropertyEnum.ONE);

        Assert.assertEquals(PropertyEnum.ONE, propertyValue);
    }

    @Test
    public void enumValueNull() {
        
        PropertyEnum enumValue = PropertyValueUtils.getEnumValue(PropertyEnum.class, null, null);

        Assert.assertNull(enumValue);
    }

    @Test
    public void listValue() {

        testRunner.setProperty(TEST_LIST_PROPERTY, "one,two");

        List<String> list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, null);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("one", list.get(0));
        Assert.assertEquals("two", list.get(1));
    }

    @Test
    public void listValueEmpty() {

        //empty
        testRunner.setProperty(TEST_LIST_PROPERTY, "");

        List<String> list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, null);

        Assert.assertEquals(0, list.size());

        // blank
        testRunner.setProperty(TEST_LIST_PROPERTY, " ");

        list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, null);

        Assert.assertEquals(0, list.size());
    }

    @Test
    public void listValueTrim() {

        testRunner.setProperty(TEST_LIST_PROPERTY, "one, two,");

        List<String> list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, null);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("one", list.get(0));
        Assert.assertEquals("two", list.get(1));
    }

    @Test
    public void listValueSkipEmpty() {

        testRunner.setProperty(TEST_LIST_PROPERTY, "one,, ,two");

        List<String> list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, null);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals("one", list.get(0));
        Assert.assertEquals("two", list.get(1));
    }

    @Test
    public void listValueOverExpression() {

        ProcessSession processSession = testRunner.getProcessSessionFactory().createSession();
        FlowFile flowFile = processSession.create();

        Map<String, String> props = new HashMap<>();
        props.put("listValue", "three,two,one");

        flowFile = processSession.putAllAttributes(flowFile, props);

        testRunner.setProperty(TEST_LIST_PROPERTY, "${listValue}");

        List<String> list = PropertyValueUtils.getList(TEST_LIST_PROPERTY, processContext, flowFile);

        Assert.assertEquals(3, list.size());
        Assert.assertEquals("three", list.get(0));
        Assert.assertEquals("two", list.get(1));
        Assert.assertEquals("one", list.get(2));
    }

    @Test
    public void mapperOptions() throws PropertyValueUtils.IllegalConfigurationException {

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(InfluxDBUtils.MEASUREMENT, "my-measurement");
            put(InfluxDBUtils.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());
            put(InfluxDBUtils.TIMESTAMP_FIELD, "log-time");
            put(InfluxDBUtils.FIELDS, "value, value2, value3");
            put(InfluxDBUtils.TAGS, "tag, tag2");
            put(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.IGNORE.name());
            put(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());
            put(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());
            put(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());
        }};

        MockConfigurationContext context = new MockConfigurationContext(props, null, new MockVariableRegistry());

        MapperOptions options = PropertyValueUtils.getMapperOptions(context, null);

        Assert.assertNotNull(options);
        Assert.assertEquals("my-measurement", options.getMeasurement());
        Assert.assertEquals(TimeUnit.HOURS, options.getPrecision());
        Assert.assertEquals("log-time", options.getTimestamp());
        Assert.assertEquals(Arrays.asList("value", "value2", "value3"), options.getFields());
        Assert.assertEquals(Arrays.asList("tag", "tag2"), options.getTags());
        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.IGNORE, options.getMissingFields());
        Assert.assertEquals(InfluxDBUtils.MissingItemsBehaviour.FAIL, options.getMissingTags());
        Assert.assertEquals(InfluxDBUtils.ComplexFieldBehaviour.WARN, options.getComplexFieldBehaviour());
        Assert.assertEquals(InfluxDBUtils.NullValueBehaviour.FAIL, options.getNullValueBehaviour());
    }

    @Test(expected = PropertyValueUtils.IllegalConfigurationException.class)
    public void mapperOptionsEmptyFields() throws PropertyValueUtils.IllegalConfigurationException {

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(InfluxDBUtils.MEASUREMENT, "my-measurement");
            put(InfluxDBUtils.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());
            put(InfluxDBUtils.TIMESTAMP_FIELD, "log-time");
            put(InfluxDBUtils.FIELDS, "");
            put(InfluxDBUtils.TAGS, "tag, tag2");
            put(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.IGNORE.name());
            put(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());
            put(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());
            put(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());
        }};

        MockConfigurationContext context = new MockConfigurationContext(props, null, new MockVariableRegistry());

        PropertyValueUtils.getMapperOptions(context, null);
    }

    @Test(expected = PropertyValueUtils.IllegalConfigurationException.class)
    public void mapperOptionsEmptyMeasurement() throws PropertyValueUtils.IllegalConfigurationException {

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(InfluxDBUtils.MEASUREMENT, "");
            put(InfluxDBUtils.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());
            put(InfluxDBUtils.TIMESTAMP_FIELD, "log-time");
            put(InfluxDBUtils.FIELDS, "value, value2, value3");
            put(InfluxDBUtils.TAGS, "tag, tag2");
            put(InfluxDBUtils.MISSING_FIELD_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.IGNORE.name());
            put(InfluxDBUtils.MISSING_TAG_BEHAVIOR, InfluxDBUtils.MissingItemsBehaviour.FAIL.name());
            put(InfluxDBUtils.COMPLEX_FIELD_BEHAVIOR, InfluxDBUtils.ComplexFieldBehaviour.WARN.name());
            put(InfluxDBUtils.NULL_VALUE_BEHAVIOR, InfluxDBUtils.NullValueBehaviour.FAIL.name());
        }};

        MockConfigurationContext context = new MockConfigurationContext(props, null, new MockVariableRegistry());

        PropertyValueUtils.getMapperOptions(context, null);
    }

    public static class TestProcessor extends AbstractProcessor {

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(TEST_ENUM_PROPERTY);
            descriptors.add(TEST_LIST_PROPERTY);

            return descriptors;
        }
    }
}
