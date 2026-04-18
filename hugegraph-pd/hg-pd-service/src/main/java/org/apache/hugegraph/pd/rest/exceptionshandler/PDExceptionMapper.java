package org.apache.hugegraph.pd.rest.exceptionshandler;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.rest.response.ApiResponse;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class PDExceptionMapper implements ExceptionMapper<PDException> {

    @Override
    public Response toResponse(PDException exception) {

        Response.Status status = getStatusCode(exception.getErrorCode());
        String reasonPhrase = status.getReasonPhrase();

        ApiResponse<Object> apiResponse = new ApiResponse<>(
                exception.getErrorCode(),
                exception.getMessage(),
                null,
                reasonPhrase);

        return Response.status(status)
                .type(MediaType.APPLICATION_JSON)
                .entity(apiResponse)
                .build();
    }

    private Response.Status getStatusCode(int code) {
        
        Response.Status status = Response.Status.fromStatusCode(code);

        if (status != null) {
            return status;
        }

        if (code >= 400 && code < 500) {
            return Response.Status.BAD_REQUEST;
        }

        if (code >= 500 && code < 600) {
            return Response.Status.INTERNAL_SERVER_ERROR;
        }

        return Response.Status.INTERNAL_SERVER_ERROR;
    }
}
