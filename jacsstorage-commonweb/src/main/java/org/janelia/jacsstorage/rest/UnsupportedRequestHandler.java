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
public class UnsupportedRequestHandler implements ExceptionMapper<UnsupportedOperationException> {
    private static final Logger LOG = LoggerFactory.getLogger(UnsupportedRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(UnsupportedOperationException exception) {
        LOG.error("Illegal access response for {}", request.getRequestURI(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.BAD_REQUEST);
        if (StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            responseBuilder.header("Content-Length", 0);
        } else {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Unsupported operation");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
