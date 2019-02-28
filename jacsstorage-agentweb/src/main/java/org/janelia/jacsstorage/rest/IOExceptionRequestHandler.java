package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

@Provider
public class IOExceptionRequestHandler implements ExceptionMapper<IOException> {
    private static final Logger LOG = LoggerFactory.getLogger(IOExceptionRequestHandler.class);

    @Override
    public Response toResponse(IOException exception) {
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
