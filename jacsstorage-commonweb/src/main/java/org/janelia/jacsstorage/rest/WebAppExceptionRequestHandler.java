package org.janelia.jacsstorage.rest;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class WebAppExceptionRequestHandler implements ExceptionMapper<WebApplicationException> {
    private static final Logger LOG = LoggerFactory.getLogger(WebAppExceptionRequestHandler.class);

    @Override
    public Response toResponse(WebApplicationException exception) {
        LOG.error("WebApplication exception", exception);
        return exception.getResponse();
    }

}
