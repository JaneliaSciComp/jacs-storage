package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

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
        if (StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            responseBuilder.header("Content-Length", 0);
        } else {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid argument");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
