package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class InvalidArgumentRequestHandler implements ExceptionMapper<IllegalArgumentException> {
    private static final Logger LOG = LoggerFactory.getLogger(InvalidArgumentRequestHandler.class);

    @Override
    public Response toResponse(IllegalArgumentException exception) {
        LOG.error("Illegal state response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Resource not found";
        }
        return Response
                .status(Response.Status.NOT_FOUND)
                .entity(ImmutableMap.of("errormessage", errorMessage))
                .build();
    }

}
