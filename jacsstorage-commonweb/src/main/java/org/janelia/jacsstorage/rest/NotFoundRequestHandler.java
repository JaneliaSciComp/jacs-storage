package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class NotFoundRequestHandler implements ExceptionMapper<NotFoundException> {
    private static final Logger LOG = LoggerFactory.getLogger(NotFoundRequestHandler.class);

    @Override
    public Response toResponse(NotFoundException exception) {
        LOG.error("Invalid argument response", exception);
        String errorMessage = exception.getMessage();
        return Response
                .status(Response.Status.NOT_FOUND)
                .entity(new ErrorResponse(errorMessage))
                .header("Content-Length", 0)
                .build();
    }

}
