package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class InvalidJsonRequestHandler implements ExceptionMapper<InvalidFormatException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Override
    public Response toResponse(InvalidFormatException exception) {
        LOG.error("Error parsing JSON response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Invalid JSON request body";
        }
        return Response
                .status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}
