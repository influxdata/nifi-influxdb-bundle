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

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

final class WriteOptions implements Cloneable {

    private String database;
    private String retentionPolicy;
    private MapperOptions mapperOptions = new MapperOptions();


    /**
     * @param database Name of database
     * @return immutable instance
     * @see PutInfluxDatabaseRecord#DB_NAME
     */
    @NonNull
    public WriteOptions database(@NonNull final String database) {

        Objects.requireNonNull(database, "Database name is required");

        WriteOptions clone = clone();
        clone.database = database;

        return clone;
    }

    /**
     * @param retentionPolicy Name of retention policy
     * @return immutable instance
     * @see PutInfluxDatabaseRecord#RETENTION_POLICY
     */
    @NonNull
    public WriteOptions setRetentionPolicy(@NonNull final String retentionPolicy) {

        Objects.requireNonNull(retentionPolicy, "Retention policy is required");

        WriteOptions clone = clone();
        clone.retentionPolicy = retentionPolicy;

        return clone;
    }

    /**
     * @param mapperOptions The options for {@link RecordToPointMapper}.
     * @return immutable instance
     */
    @NonNull
    public WriteOptions mapperOptions(@NonNull final MapperOptions mapperOptions) {

        Objects.requireNonNull(mapperOptions, "MapperOptions is required");

        WriteOptions clone = clone();
        clone.mapperOptions = mapperOptions;

        return clone;
    }

    /**
     * @see PutInfluxDatabaseRecord#DB_NAME
     */
    @NonNull
    public String getDatabase() {
        return database;
    }

    /**
     * @see PutInfluxDatabaseRecord#RETENTION_POLICY
     */
    @NonNull
    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    /**
     * @return The options for {@link RecordToPointMapper}.
     */
    @NonNull
    public MapperOptions getMapperOptions() {
        return mapperOptions;
    }

    @Override
    protected WriteOptions clone() {

        try {
            return (WriteOptions) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
