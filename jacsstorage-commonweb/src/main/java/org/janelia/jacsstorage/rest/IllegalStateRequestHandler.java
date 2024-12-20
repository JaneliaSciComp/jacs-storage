package org.janelia.jacsstorage.rest;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class IllegalStateRequestHandler implements ExceptionMapper<IllegalStateException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(IllegalStateException exception) {
        LOG.error("Illegal state for {}", request.getMethod(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.INTERNAL_SERVER_ERROR);
        if (!StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Server state error");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
