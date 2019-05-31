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
public class IllegalStateRequestHandler implements ExceptionMapper<IllegalStateException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(IllegalStateException exception) {
        LOG.error("Illegal state for {}", request.getMethod(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.INTERNAL_SERVER_ERROR);
        if (StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            responseBuilder.header("Content-Length", 0);
        } else {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Server state error");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
