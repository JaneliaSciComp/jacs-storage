package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class NotFoundRequestHandler implements ExceptionMapper<NotFoundException> {
    private static final Logger LOG = LoggerFactory.getLogger(NotFoundRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(NotFoundException exception) {
        LOG.error("No handler found for path: {}", request.getRequestURI());
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.NOT_FOUND);
        if (!StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid request");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
