package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@RequestScoped
public class NoContentFoundRequestHandler implements ExceptionMapper<NoContentFoundException> {
    private static final Logger LOG = LoggerFactory.getLogger(NoContentFoundRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(NoContentFoundException exception) {
        LOG.error("No content found for path: {}", request != null ? request.getRequestURI() : "<null request>");
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.NOT_FOUND);
        if (request == null || !StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Invalid request");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
