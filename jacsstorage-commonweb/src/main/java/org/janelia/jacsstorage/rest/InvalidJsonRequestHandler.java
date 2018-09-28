package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class InvalidJsonRequestHandler implements ExceptionMapper<InvalidFormatException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(InvalidFormatException exception) {
        LOG.error("Error parsing JSON for {}", request.getRequestURI(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.BAD_REQUEST);
        if (StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            responseBuilder.header("Content-Length", 0);
        } else {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid JSON request body");
            responseBuilder.entity(new ErrorResponse(errorMessage));
        }
        return responseBuilder.build();
    }

}
