/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.controller.exceptionhandlers;

import org.apache.hugegraph.rest.response.ApiResponse;
import org.apache.hugegraph.store.util.HgStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.logging.Level;
import java.util.logging.Logger;

@RestControllerAdvice
public class StoreExceptionHandler {

    private static final Logger logger = Logger.getLogger(StoreExceptionHandler.class.getName());

    @ExceptionHandler(HgStoreException.class)
    public ResponseEntity<ApiResponse<Object>> handleHgStoreException(HgStoreException exception) {
        int errorCode = exception.getCode();
        HttpStatus status;

        if (errorCode >= 1200 && errorCode < 1300) {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            logger.log(Level.SEVERE,
                       "Critical error at database (Code " + errorCode + "): " + exception.getMessage(), exception);
        } else if (errorCode >= 1000 && errorCode < 1200) {
            status = HttpStatus.BAD_REQUEST;

            logger.log(Level.WARNING,
                       "Validation failed at store (Code " + errorCode + "): " + exception.getMessage(), exception);
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;

            logger.log(Level.SEVERE,
                       "Unexpected error at store (Code " + errorCode + "): " + exception.getMessage(), exception);
        }

        ApiResponse<Object> apiResponse = new ApiResponse<>(
                status.value(),
                exception.getMessage(),
                null,
                status.getReasonPhrase()
        );

        return ResponseEntity.status(status).body(apiResponse);
    }
}
