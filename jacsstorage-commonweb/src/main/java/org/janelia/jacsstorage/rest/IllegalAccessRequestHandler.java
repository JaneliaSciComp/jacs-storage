package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class IllegalAccessRequestHandler implements ExceptionMapper<SecurityException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalAccessRequestHandler.class);

    @Override
    public Response toResponse(SecurityException exception) {
        LOG.error("Illegal access response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Access denied";
        }
        return Response
                .status(Response.Status.FORBIDDEN)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}
