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

package org.apache.hugegraph.pd.rest.exceptionshandler;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.rest.response.ApiResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class PDExceptionMapper {

     private static final Logger logger = LogManager.getLogger(PDExceptionMapper.class);

    @ExceptionHandler(PDException.class)
    public ResponseEntity<ApiResponse<Object>> toResponse(PDException exception) {

         logger.error(exception.getMessage(), exception);

        HttpStatus status = resolveStatus(exception.getErrorCode());
        String reasonPhrase = status.getReasonPhrase();

        ApiResponse<Object> apiResponse = new ApiResponse<>(
                exception.getErrorCode(),
                exception.getMessage(),
                null,
                reasonPhrase);

        return ResponseEntity
                .status(status)
                .body(apiResponse);

    }

    private HttpStatus resolveStatus(int code) {
        try {
            // Tenta mapear códigos HTTP exatos (ex: 400, 404, 500)
            return HttpStatus.valueOf(code);
        } catch (IllegalArgumentException e) {
            // Se falhar (ex: 4001), extraímos o primeiro dígito para descobrir a família do erro
            String codeStr = String.valueOf(code);

            if (codeStr.startsWith("4")) {
                return HttpStatus.BAD_REQUEST; // Erros de cliente -> 400
            }

            // Tudo que for da família 5000 ou não reconhecido vira Erro de Servidor -> 500
            return HttpStatus.INTERNAL_SERVER_ERROR;
        }
    }
}
