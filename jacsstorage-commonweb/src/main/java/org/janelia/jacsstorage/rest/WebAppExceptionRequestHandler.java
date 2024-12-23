package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.RequestScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@RequestScoped
public class WebAppExceptionRequestHandler implements ExceptionMapper<WebApplicationException> {
    private static final Logger LOG = LoggerFactory.getLogger(WebAppExceptionRequestHandler.class);

    @Override
    public Response toResponse(WebApplicationException exception) {
        LOG.error("WebApplication exception", exception);
        return exception.getResponse();
    }

}
