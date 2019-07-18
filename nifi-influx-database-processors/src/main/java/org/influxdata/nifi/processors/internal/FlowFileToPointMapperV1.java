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
package org.influxdata.nifi.processors.internal;

import java.util.List;
import java.util.Objects;

import org.influxdata.nifi.processors.RecordToPointMapper;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.record.Record;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 09:33)
 */
public final class FlowFileToPointMapperV1 extends AbstractFlowFileToPointMapper<Point> {

    @NonNull
    public static FlowFileToPointMapperV1 createMapper(@NonNull final ProcessSession session,
                                                       @NonNull final ProcessContext context,
                                                       @NonNull final ComponentLog log,
                                                       @NonNull final WriteOptions options) {
        return new FlowFileToPointMapperV1(session, context, log, options);
    }

    private FlowFileToPointMapperV1(@NonNull final ProcessSession session,
                                    @NonNull final ProcessContext context,
                                    @NonNull final ComponentLog log,
                                    @NonNull final WriteOptions options) {
        super(session, context, log, options);
    }

    @NonNull
    public FlowFileToPointMapperV1 writeToInflux(@NonNull final InfluxDB influxDB) {

        Objects.requireNonNull(flowFile, "FlowFile is required");
        Objects.requireNonNull(points, "Points are required");

        stopWatch.start();

        if (influxDB.isBatchEnabled()) {

            // Write by batching
            points.forEach(point -> influxDB.write(options.getDatabase(), options.getRetentionPolicy(), point));

        } else {

            BatchPoints batch = BatchPoints
                    .database(options.getDatabase())
                    .retentionPolicy(options.getRetentionPolicy())
                    .build();

            points.forEach(batch::point);

            // Write all Points together
            influxDB.write(batch);
        }

        stopWatch.stop();

        return this;
    }

    @NonNull
    public FlowFileToPointMapperV1 addFlowFile(@Nullable final FlowFile flowFile) throws Exception {
        super.mapFlowFile(flowFile);
        return this;
    }

    @NonNull
    @Override
    List<Point> mapRecord(final RecordToPointMapper toPointMapper, final Record record) {
        return toPointMapper.mapRecord(record);
    }
}