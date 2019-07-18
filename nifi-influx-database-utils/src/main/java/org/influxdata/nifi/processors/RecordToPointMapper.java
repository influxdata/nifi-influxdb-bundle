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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.influxdata.client.domain.WritePrecision;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.influxdb.dto.Point;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.influxdata.nifi.util.InfluxDBUtils.FIELD_NULL_VALUE;
import static org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour;
import static org.influxdata.nifi.util.InfluxDBUtils.NullValueBehaviour;
import static org.influxdata.nifi.util.InfluxDBUtils.REQUIRED_FIELD_MISSING;
import static org.influxdata.nifi.util.InfluxDBUtils.UNSUPPORTED_FIELD_TYPE;

/**
 * @author Jakub Bednar (bednar@github) (29/05/2019 10:52)
 */
public final class RecordToPointMapper {

    private final MapperOptions options;
    private final RecordSchema schema;
    private final ComponentLog log;

    public RecordToPointMapper(final MapperOptions options, RecordSchema schema, ComponentLog log) {
        this.options = options;
        this.schema = schema;
        this.log = log;
    }

    @NonNull
    public List<Point> mapRecord(@Nullable final Record record) {

        if (record == null) {
            return new ArrayList<>();
        }

        // If the Record contains a field with measurement property value,
        // then value of the Record field is use as InfluxDB measurement.
        String measurement = record.getAsString(options.getMeasurement());
        if (StringUtils.isBlank(measurement)) {

            measurement = options.getMeasurement();
        }

        PointBuilder<Point> point = PointBuilderV1.measurement(measurement);

        return mapRecord(record, point);
    }

    @NonNull
    public List<org.influxdata.client.write.Point > mapRecordV2(@Nullable final Record record) {

        if (record == null) {
            return new ArrayList<>();
        }

        // If the Record contains a field with measurement property value,
        // then value of the Record field is use as InfluxDB measurement.
        String measurement = record.getAsString(options.getMeasurement());
        if (StringUtils.isBlank(measurement)) {

            measurement = options.getMeasurement();
        }

        PointBuilder<org.influxdata.client.write.Point> point = PointBuilderV2.measurement(measurement);

        return mapRecord(record, point);
    }

    private <P> List<P> mapRecord(@NonNull final Record record, final @NonNull PointBuilder<P> point) {

        List<P> points = new ArrayList<>();
        mapFields(record, point);

        if (point.hasFields()) {

            mapTags(record, point);
            mapTimestamp(record, point);

            points.add(point.build());
        }

        return points;
    }

    private void mapFields(@Nullable final Record record, @NonNull final PointBuilder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        RecordFieldMapper recordFieldMapper = new RecordFieldMapper(record, point);

        options.getFields().forEach(fieldName -> {

            RecordField recordField = findRecordField(fieldName, options.getMissingFields());
            if (recordField != null) {

                recordFieldMapper.apply(recordField);
            }
        });
    }

    private void mapTags(@Nullable final Record record, @NonNull final PointBuilder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        SaveKeyValue saveTag = (key, value, dataType) -> {

            Objects.requireNonNull(key, "Tag Key is required");
            Objects.requireNonNull(dataType, "Tag Value Type is required");

            String stringValue = DataTypeUtils.toString(value, dataType.getFormat());
            if (stringValue == null) {
                return;
            }

            point.tag(key, stringValue);
        };


        for (String tag : options.getTags()) {

            RecordField recordField = findRecordField(tag, options.getMissingTags());
            if (recordField == null) {
                continue;
            }

            Object recordValue = record.getValue(recordField);
            if (recordValue == null) {
                nullBehaviour(tag);
                continue;
            }

            DataType dataType = recordField.getDataType();

            mapTag(tag, recordValue, dataType, saveTag);
        }
    }

    private void mapTimestamp(@Nullable final Record record, @NonNull final PointBuilder point) {

        Objects.requireNonNull(point, "Point is required");

        if (record == null) {
            return;
        }

        RecordField recordField = findRecordField(options.getTimestamp(), MissingItemsBehaviour.IGNORE);
        if (recordField == null) {
            return;
        }

        Object value = record.getValue(recordField);
        if (value == null) {
            return;
        }

        Long time;
        TimeUnit precision;
        WritePrecision writePrecision;

        time = DataTypeUtils.toLong(value, recordField.getFieldName());
        if (value instanceof Date) {
            precision = MILLISECONDS;
            writePrecision = WritePrecision.MS;
        } else {
            precision = options.getPrecision();
            writePrecision = options.getWritePrecision();
        }

        point.time(time, precision, writePrecision);
    }

    @Nullable
    private RecordField findRecordField(@Nullable final String fieldName,
                                        @NonNull final MissingItemsBehaviour missingBehaviour) {

        Objects.requireNonNull(missingBehaviour, "MissingItemsBehaviour for not defined items is required");

        RecordField recordField = schema.getField(fieldName).orElse(null);

        // missing field + FAIL => exception
        if (recordField == null && MissingItemsBehaviour.FAIL.equals(missingBehaviour)) {

            String message = String.format(REQUIRED_FIELD_MISSING, fieldName);

            throw new IllegalStateException(message);
        }

        return recordField;
    }

    private void unsupportedType(@NonNull final DataType dataType, @NonNull final String fieldName) {

        Objects.requireNonNull(fieldName, "fieldName is required");
        Objects.requireNonNull(dataType, "DataType is required");

        RecordFieldType fieldType = dataType.getFieldType();

        String message = String.format(UNSUPPORTED_FIELD_TYPE, fieldName, fieldType);

        throw new IllegalStateException(message);
    }

    private void handleComplexField(@Nullable final Object value,
                                    @NonNull final DataType fieldType,
                                    @NonNull final String fieldName,
                                    @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(fieldType, "RecordFieldType is required.");
        Objects.requireNonNull(fieldName, "FieldName is required.");

        switch (options.getComplexFieldBehaviour()) {

            case FAIL:
                String message = String.format("Complex value found for %s; routing to failure", fieldName);
                log.error(message);
                throw new IllegalStateException(message);

            case WARN:
                log.warn("Complex value found for {}; skipping", new Object[]{fieldName});
                break;

            case TEXT:
                String stringValue = DataTypeUtils.toString(value, fieldType.getFormat());

                saveKeyValue.save(fieldName, stringValue, RecordFieldType.STRING.getDataType());
                break;
            case IGNORE:
                // skip
                break;
        }
    }

    private interface SaveKeyValue {
        void save(@NonNull final String key, @Nullable final Object value, @NonNull final DataType dataType);
    }

    private final class RecordFieldMapper {

        private final Record record;
        private final PointBuilder point;

        private RecordFieldMapper(@NonNull final Record record,
                                  @NonNull final PointBuilder point) {
            this.record = record;
            this.point = point;
        }

        void apply(@NonNull final RecordField recordField) {

            String field = recordField.getFieldName();
            Object value = record.getValue(recordField);

            addField(field, value, recordField.getDataType());
        }

        private void addField(@NonNull final String field,
                              @Nullable final Object value,
                              @NonNull final DataType dataType) {

            if (value == null) {
                nullBehaviour(field);
                return;
            }

            DataType defaultValueType = RecordFieldType.CHOICE.getChoiceDataType(
                    RecordFieldType.FLOAT.getDataType(),
                    RecordFieldType.LONG.getDataType(),
                    RecordFieldType.BOOLEAN.getDataType(),
                    RecordFieldType.STRING.getDataType());

            switch (dataType.getFieldType()) {

                case DATE:
                case TIME:
                case TIMESTAMP:
                case LONG:
                    Long time = DataTypeUtils.toLong(value, field);
                    point.addField(field, time);
                    break;

                case DOUBLE:
                    Double doubleNumber = DataTypeUtils.toDouble(value, field);
                    point.addField(field, doubleNumber);
                    break;

                case FLOAT:
                    Float floatNumber = DataTypeUtils.toFloat(value, field);
                    point.addField(field, floatNumber);
                    break;

                case INT:
                case BYTE:
                case SHORT:
                    Integer integerNumber = DataTypeUtils.toInteger(value, field);
                    point.addField(field, integerNumber);
                    break;

                case BIGINT:
                    BigInteger bigIntegerNumber = DataTypeUtils.toBigInt(value, field);
                    point.addField(field, bigIntegerNumber);
                    break;

                case BOOLEAN:
                    Boolean bool = DataTypeUtils.toBoolean(value, field);
                    point.addField(field, bool);
                    break;

                case CHAR:
                case STRING:
                    String stringValue = value.toString();
                    point.addField(field, stringValue);
                    break;

                case CHOICE:
                    DataType choiceDataType = DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType);
                    if (choiceDataType == null) {
                        choiceDataType = defaultValueType;
                    }

                    Object choiceValue = DataTypeUtils.convertType(value, choiceDataType, field);

                    addField(field, choiceValue, choiceDataType);
                    break;

                case MAP:

                    DataType mapValueType = ((MapDataType) dataType).getValueType();
                    if (mapValueType == null) {
                        mapValueType = defaultValueType;
                    }

                    Map<String, Object> map = DataTypeUtils.toMap(value, field);

                    storeMap(map, mapValueType, this::addField);

                    break;

                case ARRAY:
                case RECORD:
                    handleComplexField(value, dataType, field, this::addField);
                    break;

                default:
                    unsupportedType(dataType, field);
            }
        }
    }

    private void mapTag(@NonNull final String tag,
                        @Nullable final Object value,
                        @NonNull final DataType type,
                        @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(tag, "Tag is required");
        Objects.requireNonNull(type, "DataType is required");
        Objects.requireNonNull(saveKeyValue, "SaveKeyValue is required");

        RecordFieldType defaultDataType = RecordFieldType.STRING;

        switch (type.getFieldType()) {

            case STRING:
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case BIGINT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
                saveKeyValue.save(tag, value, type);
                break;

            case CHOICE:
                DataType choiceValueType = DataTypeUtils.chooseDataType(value, (ChoiceDataType) type);
                if (choiceValueType == null) {
                    choiceValueType = defaultDataType.getDataType();
                }

                Object choiceValue = DataTypeUtils.convertType(value, choiceValueType, tag);

                mapTag(tag, choiceValue, choiceValueType, saveKeyValue);
                break;

            case MAP:

                DataType mapValueType = ((MapDataType) type).getValueType();
                if (mapValueType == null) {
                    mapValueType = defaultDataType.getDataType();
                }

                Map<String, Object> map = DataTypeUtils.toMap(value, tag);
                storeMap(map, mapValueType, saveKeyValue);

                break;

            case ARRAY:
            case RECORD:
                handleComplexField(value, type, tag, saveKeyValue);
                break;

            default:
                unsupportedType(type, tag);
        }
    }

    private void storeMap(@Nullable final Map<String, ?> map,
                          @NonNull final DataType mapValueType,
                          @NonNull final SaveKeyValue saveKeyValue) {

        Objects.requireNonNull(mapValueType, "MapValueType is required");
        Objects.requireNonNull(saveKeyValue, "SaveKeyValue function is required");

        if (map == null) {
            return;
        }

        for (final String mapKey : map.keySet()) {

            Object rawValue = map.get(mapKey);
            DataType rawType = mapValueType;

            if (mapValueType.getFieldType() == RecordFieldType.CHOICE) {

                rawType = DataTypeUtils.chooseDataType(rawValue, (ChoiceDataType) mapValueType);
            }

            if (rawType == null) {
                String message = String.format("Map does not have a specified type of values. MapValueType: %s", mapValueType);

                throw new IllegalStateException(message);
            }

            Object mapValue = DataTypeUtils.convertType(rawValue, rawType, mapKey);
            saveKeyValue.save(mapKey, mapValue, rawType);
        }
    }

    private void nullBehaviour(@NonNull final String fieldName) {

        Objects.requireNonNull(fieldName, "Field name is required");

        if (NullValueBehaviour.FAIL.equals(options.getNullValueBehaviour())) {
            String message = String.format(FIELD_NULL_VALUE, fieldName);

            throw new IllegalStateException(message);
        }
    }

    private static abstract class PointBuilder<P> {

        public abstract void addField(final String field, final Long value);
        public abstract void addField(final String field, final Double value);
        public abstract void addField(final String field, final Float value);
        public abstract void addField(final String field, final Integer value);
        public abstract void addField(final String field, final BigInteger value);
        public abstract void addField(final String field, final Boolean value);
        public abstract void addField(final String field, final String value);

        public abstract void tag(final String key, final String value);

        public abstract void time(final Long time, final TimeUnit precision, final WritePrecision writePrecision);

        public abstract boolean hasFields();

        public abstract P build();
    }

    private static class PointBuilderV1 extends PointBuilder<Point> {

        private Point.Builder builder;

        private PointBuilderV1(final String measurement) {
            this.builder = Point.measurement(measurement);
        }

        public static PointBuilderV1 measurement(final String measurement) {

            return new PointBuilderV1(measurement);
        }

        @Override
        public void addField(final String field, final Long value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final Double value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final Float value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final Integer value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final BigInteger value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final Boolean value) {
            builder.addField(field, value);
        }
        @Override
        public void addField(final String field, final String value) {
            builder.addField(field, value);
        }

        @Override
        public void tag(final String key, final String value) {
            builder.tag(key, value);
        }

        @Override
        public void time(final Long time, final TimeUnit precision, final WritePrecision writePrecision) {
            builder.time(time, precision);
        }

        @Override
        public boolean hasFields() {
            return builder.hasFields();
        }

        @Override
        public Point build() {
            return builder.build();
        }
    }

    private static class PointBuilderV2 extends PointBuilder<org.influxdata.client.write.Point> {

        private org.influxdata.client.write.Point point;

        public static PointBuilderV2 measurement(final String measurement) {

            return new PointBuilderV2(measurement);
        }

        private PointBuilderV2(final String measurement) {
            this.point = org.influxdata.client.write.Point.measurement(measurement);
        }

        @Override
        public void addField(final String field, final Long value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final Double value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final Float value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final Integer value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final BigInteger value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final Boolean value) {
            point.addField(field, value);
        }

        @Override
        public void addField(final String field, final String value) {
            point.addField(field, value);
        }

        @Override
        public void tag(final String key, final String value) {
            point.addTag(key, value);
        }

        @Override
        public void time(final Long time, final TimeUnit precision, final WritePrecision writePrecision) {
            point.time(time, writePrecision);
        }

        @Override
        public boolean hasFields() {
            return point.hasFields();
        }

        @Override
        public org.influxdata.client.write.Point build() {
            return point;
        }
    }
}