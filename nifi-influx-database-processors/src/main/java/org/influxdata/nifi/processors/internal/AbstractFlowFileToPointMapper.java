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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.influxdata.nifi.processors.PutInfluxDatabaseRecord;
import org.influxdata.nifi.processors.RecordToPointMapper;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractFlowFileToPointMapper<P> {

    private final ComponentLog log;
    private final ProcessSession session;
    private final ProcessContext context;

    final WriteOptions options;

    FlowFile flowFile;

    StopWatch stopWatch = new StopWatch();
    List<P> points = new ArrayList<>();

    AbstractFlowFileToPointMapper(@NonNull final ProcessSession session,
                                  @NonNull final ProcessContext context,
                                  @NonNull final ComponentLog log,
                                  @NonNull final WriteOptions options) {

        Objects.requireNonNull(session, "ProcessSession is required");
        Objects.requireNonNull(context, "ProcessContext is required");
        Objects.requireNonNull(log, "ComponentLog is required");
        Objects.requireNonNull(options, "WriteOptions is required");

        this.session = session;
        this.context = context;
        this.log = log;
        this.options = options;
    }

    void mapFlowFile(@Nullable final FlowFile flowFile) throws Exception {


        this.flowFile = flowFile;

        if (flowFile == null) {
            return;
        }

        try (final InputStream stream = session.read(flowFile)) {

            mapInputStream(stream);
        }

    }

    public void reportResults(@Nullable final String url) {

        Objects.requireNonNull(flowFile, "FlowFile is required");

        if (!points.isEmpty()) {

            String transitUri = url + "/" + (options.getDatabase() != null ? options.getDatabase() : "");
            String message = String.format("Added %d points to InfluxDB.", points.size());

            session.getProvenanceReporter().send(flowFile, transitUri, message, stopWatch.getElapsed(MILLISECONDS));
        } else {

            String flowFileName = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());

            log.info("The all fields of FlowFile={} has null value. There is nothing to store to InfluxDB",
                    new Object[]{flowFileName});
        }
    }

    private void mapInputStream(@NonNull final InputStream stream) throws
            IOException, MalformedRecordException, SchemaNotFoundException {

        Objects.requireNonNull(flowFile, "FlowFile is required");

        RecordReaderFactory factory = context
                .getProperty(PutInfluxDatabaseRecord.RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        RecordReader parser = factory
                .createRecordReader(flowFile, stream, log);

        RecordSchema schema = parser.getSchema();

        RecordToPointMapper toPointMapper = new RecordToPointMapper(options.getMapperOptions(), schema, log);

        Record record;
        while ((record = parser.nextRecord()) != null) {

            points.addAll(mapRecord(toPointMapper, record));
        }
    }

    @NonNull
    abstract List<P> mapRecord(final RecordToPointMapper toPointMapper, final Record record);

}
