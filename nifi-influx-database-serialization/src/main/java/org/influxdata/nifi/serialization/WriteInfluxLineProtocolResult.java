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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import org.influxdata.nifi.processors.MapperOptions;
import org.influxdata.nifi.processors.RecordToPointMapper;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.influxdb.dto.Point;

/**
 * @author Jakub Bednar (bednar@github) (29/05/2019 10:26)
 */
public class WriteInfluxLineProtocolResult extends AbstractRecordSetWriter {

    private static final byte NEW_LINE = (byte) '\n';

    private final RecordSchema recordSchema;
    private final ComponentLog componentLog;
    private final MapperOptions mapperOptions;
    private final String charSet;

    WriteInfluxLineProtocolResult(final OutputStream out,
                                  final RecordSchema recordSchema,
                                  final ComponentLog componentLog,
                                  final MapperOptions mapperOptions,
                                  final String charSet) {
        super(out);

        this.recordSchema = recordSchema;
        this.componentLog = componentLog;
        this.mapperOptions = mapperOptions;
        this.charSet = charSet;
    }

    @Override
    public String getMimeType() {
        return "text/plain";
    }

    @Override
    protected Map<String, String> writeRecord(final Record record) throws IOException {

        RecordToPointMapper toPointMapper = new RecordToPointMapper(mapperOptions, recordSchema, componentLog);

        OutputStream out = getOutputStream();

        for (Point point : toPointMapper.mapRecord(record)) {

            out.write(point.lineProtocol(mapperOptions.getPrecision()).getBytes(charSet));
            out.write(NEW_LINE);
        }

        return Collections.emptyMap();
    }
}