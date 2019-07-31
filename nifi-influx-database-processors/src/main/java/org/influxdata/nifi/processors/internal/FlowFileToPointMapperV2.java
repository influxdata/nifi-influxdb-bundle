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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import org.influxdata.nifi.processors.RecordToPointMapper;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.record.Record;

/**
 * @author Jakub Bednar (bednar@github) (18/07/2019 09:33)
 */
public final class FlowFileToPointMapperV2 extends AbstractFlowFileToPointMapper<Point> {

    @NonNull
    public static FlowFileToPointMapperV2 createMapper(@NonNull final ProcessSession session,
                                                       @NonNull final ProcessContext context,
                                                       @NonNull final ComponentLog log,
                                                       @NonNull final WriteOptions options) {
        return new FlowFileToPointMapperV2(session, context, log, options);
    }

    private FlowFileToPointMapperV2(@NonNull final ProcessSession session,
                                    @NonNull final ProcessContext context,
                                    @NonNull final ComponentLog log,
                                    @NonNull final WriteOptions options) {
        super(session, context, log, options);
    }

    @NonNull
    public FlowFileToPointMapperV2 writeToInflux(@NonNull final String bucket,
                                                 @NonNull final String org,
                                                 @NonNull final InfluxDBClient influxDBClient) {

        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(org, "org");
        Objects.requireNonNull(flowFile, "FlowFile is required");
        Objects.requireNonNull(points, "Points are required");

        stopWatch.start();

        influxDBClient.getWriteApiBlocking().writePoints(bucket, org, points);

        stopWatch.stop();

        return this;
    }

    @NonNull
    public FlowFileToPointMapperV2 addFlowFile(@Nullable final FlowFile flowFile) throws Exception {
        super.mapFlowFile(flowFile);
        return this;
    }

    @NonNull
    @Override
    List<Point> mapRecord(final RecordToPointMapper toPointMapper, final Record record) {
        return toPointMapper.mapRecordV2(record);
    }
}