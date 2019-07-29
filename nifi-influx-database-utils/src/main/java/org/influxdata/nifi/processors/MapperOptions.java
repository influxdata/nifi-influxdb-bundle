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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.influxdata.client.domain.WritePrecision;
import org.influxdata.nifi.util.InfluxDBUtils;
import org.influxdata.nifi.util.InfluxDBUtils.ComplexFieldBehaviour;
import org.influxdata.nifi.util.InfluxDBUtils.MissingItemsBehaviour;
import org.influxdata.nifi.util.InfluxDBUtils.NullValueBehaviour;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.influxdata.nifi.util.InfluxDBUtils.COMPLEX_FIELD_BEHAVIOUR_DEFAULT;
import static org.influxdata.nifi.util.InfluxDBUtils.MISSING_FIELDS_BEHAVIOUR_DEFAULT;
import static org.influxdata.nifi.util.InfluxDBUtils.MISSING_TAGS_BEHAVIOUR_DEFAULT;
import static org.influxdata.nifi.util.InfluxDBUtils.NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT;
import static org.influxdata.nifi.util.InfluxDBUtils.PRECISION_DEFAULT;

public final class MapperOptions implements Cloneable {

    private String timestamp;
    private TimeUnit precision = PRECISION_DEFAULT;
    private WritePrecision writePrecision = WritePrecision.NS;
    private String measurement;
    private List<String> fields = new ArrayList<>();
    private MissingItemsBehaviour missingFields = MISSING_FIELDS_BEHAVIOUR_DEFAULT;
    private List<String> tags = new ArrayList<>();
    private MissingItemsBehaviour missingTags = MISSING_TAGS_BEHAVIOUR_DEFAULT;
    private ComplexFieldBehaviour complexFieldBehaviour = COMPLEX_FIELD_BEHAVIOUR_DEFAULT;
    private NullValueBehaviour nullValueBehaviour = NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT;

    /**
     * @param timestamp A name of the record field that used as a 'timestamp'
     * @return immutable instance
     * @see InfluxDBUtils#TIMESTAMP_FIELD
     */
    @NonNull
    public MapperOptions timestamp(@Nullable final String timestamp) {

        MapperOptions clone = clone();
        clone.timestamp = timestamp;

        return clone;
    }

    /**
     * @param precision Precision of timestamp
     * @return immutable instance
     * @see InfluxDBUtils#TIMESTAMP_PRECISION
     */
    @NonNull
    public MapperOptions precision(@NonNull final TimeUnit precision) {

        Objects.requireNonNull(precision, "Precision of timestamp is required");

        MapperOptions clone = clone();
        clone.precision = precision;

        return clone;
    }

    /**
     * @param writePrecision Precision of timestamp
     * @return immutable instance
     * @see WritePrecision
     */
    @NonNull
    public MapperOptions writePrecision(@NonNull final WritePrecision writePrecision) {

        Objects.requireNonNull(writePrecision, "Precision of timestamp is required");

        MapperOptions clone = clone();
        clone.writePrecision = writePrecision;

        return clone;
    }

    /**
     * @param measurement Name of the measurement
     * @return immutable instance
     * @see InfluxDBUtils#MEASUREMENT
     */
    @NonNull
    public MapperOptions measurement(@NonNull final String measurement) {

        Objects.requireNonNull(measurement, "Name of the measurement is required");

        MapperOptions clone = clone();
        clone.measurement = measurement;

        return clone;
    }

    /**
     * @param fields Name of the fields
     * @return immutable instance
     * @see InfluxDBUtils#FIELDS
     */
    @NonNull
    public MapperOptions fields(@NonNull final List<String> fields) {

        Objects.requireNonNull(fields, "Fields are required");

        MapperOptions clone = clone();
        clone.fields.addAll(fields);

        return clone;
    }

    /**
     * @param missingFields Missing fields behaviour
     * @return immutable instance
     * @see InfluxDBUtils#MISSING_FIELD_BEHAVIOR
     */
    @NonNull
    public MapperOptions missingFields(@NonNull final MissingItemsBehaviour missingFields) {

        Objects.requireNonNull(missingFields, "Missing fields behaviour is required");

        MapperOptions clone = clone();
        clone.missingFields = missingFields;

        return clone;
    }

    /**
     * @param tags Evaluated names of the tags
     * @return immutable instance
     * @see InfluxDBUtils#TAGS
     */
    @NonNull
    public MapperOptions tags(@NonNull final List<String> tags) {

        Objects.requireNonNull(tags, "Tags are required");

        MapperOptions clone = clone();
        clone.tags.addAll(tags);

        return clone;
    }

    /**
     * @param missingTags Missing tags behaviour
     * @return immutable instance
     * @see InfluxDBUtils#MISSING_TAG_BEHAVIOR
     */
    @NonNull
    public MapperOptions missingTags(@NonNull final MissingItemsBehaviour missingTags) {

        Objects.requireNonNull(missingTags, "Missing tags behaviour is required");

        MapperOptions clone = clone();
        clone.missingTags = missingTags;

        return clone;
    }

    /**
     * @param complexFieldBehaviour Complex field behaviour
     * @return immutable instance
     * @see InfluxDBUtils#COMPLEX_FIELD_BEHAVIOR
     */
    @NonNull
    public MapperOptions complexFieldBehaviour(@NonNull final ComplexFieldBehaviour complexFieldBehaviour) {

        Objects.requireNonNull(complexFieldBehaviour, "Missing tags behaviour is required");

        MapperOptions clone = clone();
        clone.complexFieldBehaviour = complexFieldBehaviour;

        return clone;
    }

    /**
     * @param nullValueBehaviour Null Value Behaviour
     * @return immutable instance
     * @see InfluxDBUtils#NULL_VALUE_BEHAVIOR
     */
    @NonNull
    public MapperOptions nullValueBehaviour(@NonNull final NullValueBehaviour nullValueBehaviour) {

        Objects.requireNonNull(nullValueBehaviour, "Null Value Behavior is required");

        MapperOptions clone = clone();
        clone.nullValueBehaviour = nullValueBehaviour;

        return clone;
    }

    /**
     * @return Evaluated timestamp name
     * @see InfluxDBUtils#TIMESTAMP_FIELD
     */
    @Nullable
    public String getTimestamp() {

        return timestamp;
    }

    /**
     * @return Evaluated timestamp precision
     * @see InfluxDBUtils#TIMESTAMP_PRECISION
     */
    @NonNull
    public TimeUnit getPrecision() {
        return precision;
    }

    /**
     * @return Evaluated write precision
     */
    @NonNull
    public WritePrecision getWritePrecision() {
        return writePrecision;
    }

    /**
     * @return Evaluated measurement name
     * @see InfluxDBUtils#MEASUREMENT
     */
    @NonNull
    public String getMeasurement() {
        return measurement;
    }

    /**
     * @return Evaluated fields names
     * @see InfluxDBUtils#FIELDS
     */
    @NonNull
    public List<String> getFields() {
        return fields;
    }

    /**
     * @see InfluxDBUtils#MISSING_FIELD_BEHAVIOR
     */
    @NonNull
    public MissingItemsBehaviour getMissingFields() {
        return missingFields;
    }

    /**
     * @return Evaluated tags names
     * @see InfluxDBUtils#TAGS
     */
    @NonNull
    public List<String> getTags() {
        return tags;
    }

    /**
     * @see InfluxDBUtils#MISSING_TAG_BEHAVIOR
     */
    @NonNull
    public MissingItemsBehaviour getMissingTags() {
        return missingTags;
    }

    /**
     * @see InfluxDBUtils#COMPLEX_FIELD_BEHAVIOR
     */
    @NonNull
    public ComplexFieldBehaviour getComplexFieldBehaviour() {
        return complexFieldBehaviour;
    }

    /**
     * @see InfluxDBUtils#NULL_VALUE_BEHAVIOR
     */
    @NonNull
    public NullValueBehaviour getNullValueBehaviour() {
        return nullValueBehaviour;
    }

    @Override
    protected MapperOptions clone() {

        try {
            return (MapperOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
