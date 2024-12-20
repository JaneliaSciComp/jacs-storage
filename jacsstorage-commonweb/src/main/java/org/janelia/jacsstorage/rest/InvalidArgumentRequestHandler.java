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
public class InvalidArgumentRequestHandler implements ExceptionMapper<IllegalArgumentException> {
    private static final Logger LOG = LoggerFactory.getLogger(InvalidArgumentRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(IllegalArgumentException exception) {
        LOG.error("Invalid argument for {}: {}", request.getMethod(), exception.getMessage(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.NOT_FOUND);
        if (!StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid argument");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
