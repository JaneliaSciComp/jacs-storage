package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnsupportedRequestHandler implements ExceptionMapper<UnsupportedOperationException> {
    private static final Logger LOG = LoggerFactory.getLogger(UnsupportedRequestHandler.class);

    @Override
    public Response toResponse(UnsupportedOperationException exception) {
        LOG.error("Illegal access response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Unsupported operation";
        }
        return Response
                .status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}
