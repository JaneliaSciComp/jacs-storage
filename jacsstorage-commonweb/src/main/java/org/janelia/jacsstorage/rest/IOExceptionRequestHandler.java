package org.janelia.jacsstorage.rest;

import java.io.IOException;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@RequestScoped
public class IOExceptionRequestHandler implements ExceptionMapper<IOException> {
    private static final Logger LOG = LoggerFactory.getLogger(IOExceptionRequestHandler.class);

    @Override
    public Response toResponse(IOException exception) {
        LOG.error("Input/Output error", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Server input/output error";
        }
        return Response
                .serverError()
                .entity(new ErrorResponse(errorMessage))
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

}
