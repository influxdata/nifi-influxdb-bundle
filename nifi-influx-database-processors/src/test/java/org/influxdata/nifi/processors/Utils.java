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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.ResponseBody;
import retrofit2.Response;

class Utils {

    static Response<Object> createErrorResponse(final int code) {
        return createErrorResponse(code, new HashMap<>());
    }

    static Response<Object> createErrorResponse(final int code, @Nonnull final Map<String, String> headers) {
        okhttp3.Response.Builder builder = new okhttp3.Response.Builder() //
                .code(code)
                .addHeader("X-Influx-Error", "Simulate error: " + code)
                .message("Response.error()")
                .protocol(Protocol.HTTP_1_1)
                .request(new Request.Builder().url("http://localhost/").build());

        headers.forEach(builder::addHeader);

        return Response.error(ResponseBody.create("", MediaType.parse("application/json")), builder.build());
    }

}
