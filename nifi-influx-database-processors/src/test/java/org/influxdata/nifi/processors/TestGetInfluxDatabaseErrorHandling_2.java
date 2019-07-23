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

import java.net.SocketTimeoutException;
import java.util.List;

import org.influxdata.exceptions.InfluxException;
import org.influxdata.exceptions.NotFoundException;
import org.influxdata.nifi.processors.internal.AbstractGetInfluxDatabase;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Assert;
import org.junit.Test;

import static org.influxdata.nifi.processors.Utils.createErrorResponse;
import static org.influxdata.nifi.processors.internal.AbstractInfluxDatabaseProcessor.INFLUX_DB_ERROR_MESSAGE;

/**
 * @author Jakub Bednar (bednar@github) (23/07/2019 08:06)
 */
public class TestGetInfluxDatabaseErrorHandling_2 extends AbstractTestGetInfluxDatabaseSettings_2 {

    @Test
    public void socketTimeoutException() {

        SocketTimeoutException exception = new SocketTimeoutException("Simulate error: SocketTimeoutException");
        exceptionResponse(new InfluxException(exception), "Simulate error: SocketTimeoutException", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void serviceUnavailableException() {

        exceptionResponse(new InfluxException(createErrorResponse(503)), "Simulate error: 503", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void tooManyRequestsException() {

        exceptionResponse(new InfluxException(createErrorResponse(429)), "Simulate error: 429", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void notFoundException() {

        exceptionResponse(new NotFoundException(createErrorResponse(404)), "Simulate error: 404", AbstractGetInfluxDatabase.REL_FAILURE);

    }

    @Test
    public void socketTimeoutError() {

        SocketTimeoutException exception = new SocketTimeoutException("Simulate error: SocketTimeoutException");
        errorResponse(new InfluxException(exception), "Simulate error: SocketTimeoutException", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void serviceUnavailableError() {

        errorResponse(new InfluxException(createErrorResponse(503)), "Simulate error: 503", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void tooManyRequestsError() {

        errorResponse(new InfluxException(createErrorResponse(429)), "Simulate error: 429", AbstractGetInfluxDatabase.REL_RETRY);
    }

    @Test
    public void notFoundError() {

        errorResponse(new NotFoundException(createErrorResponse(404)), "Simulate error: 404", AbstractGetInfluxDatabase.REL_FAILURE);

    }

    private void exceptionResponse(@NonNull final Exception exception,
                                   @NonNull final String message, final Relationship relSuccess) {

        //
        // Query error
        //
        queryAnswer = invocation -> {
            throw exception;
        };

        verifyError(exception, message, relSuccess);
    }

    private void errorResponse(@NonNull final Exception exception,
                               @NonNull final String message, final Relationship relSuccess) {

        //
        // Query error
        //
        queryOnErrorValue = exception;

        verifyError(exception, message, relSuccess);
    }

    private void verifyError(@NonNull final Exception exception, @NonNull final String message, final Relationship relationship) {

        runner.enqueue("");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertEquals(message, flowFiles.get(0).getAttribute(INFLUX_DB_ERROR_MESSAGE));

        List<LogMessage> errors = logger.getErrorMessages();

        // First is formatted message, Second Stack Trace
        Assert.assertEquals(2, errors.size());

        Assert.assertEquals(errors.get(0).getArgs()[2], message);
        Assert.assertTrue(exception.getClass().isAssignableFrom(errors.get(1).getThrowable().getClass()));
    }
}