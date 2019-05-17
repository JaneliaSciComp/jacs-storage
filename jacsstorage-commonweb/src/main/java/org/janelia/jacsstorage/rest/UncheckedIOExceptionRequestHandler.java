package org.janelia.jacsstorage.rest;

import java.io.UncheckedIOException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class UncheckedIOExceptionRequestHandler implements ExceptionMapper<UncheckedIOException> {
    private static final Logger LOG = LoggerFactory.getLogger(UncheckedIOExceptionRequestHandler.class);

    @Override
    public Response toResponse(UncheckedIOException exception) {
        LOG.error("Illegal state response", exception);
        String errorMessage;
        if (StringUtils.isBlank(exception.getMessage())) {
            errorMessage = "Server state error";
        } else {
            errorMessage = exception.getMessage();
        }
        return Response
                .serverError()
                .entity(ImmutableMap.of("errormessage", errorMessage))
                .build();
    }

}
