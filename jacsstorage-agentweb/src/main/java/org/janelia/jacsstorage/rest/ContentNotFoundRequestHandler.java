package org.janelia.jacsstorage.rest;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class ContentNotFoundRequestHandler implements ExceptionMapper<NoContentFoundException> {
    private static final Logger LOG = LoggerFactory.getLogger(ContentNotFoundRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(NoContentFoundException exception) {
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.NOT_FOUND);
        String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "No content found");
        responseBuilder
                .entity(new ErrorResponse(errorMessage))
                .type(MediaType.APPLICATION_JSON);
        return responseBuilder.build();
    }

}
