package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class IllegalStateRequestHandler implements ExceptionMapper<IllegalStateException> {

    @Override
    public Response toResponse(IllegalStateException exception) {
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Server state error";
        }
        return Response
                .serverError()
                .entity(ImmutableMap.of("errormessage", errorMessage))
                .build();
    }

}
