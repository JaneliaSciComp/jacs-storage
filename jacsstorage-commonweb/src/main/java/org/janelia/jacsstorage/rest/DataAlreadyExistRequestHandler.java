package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.DataAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DataAlreadyExistRequestHandler implements ExceptionMapper<DataAlreadyExistException> {
    private static final Logger LOG = LoggerFactory.getLogger(DataAlreadyExistRequestHandler.class);

    @Context
    private HttpServletRequest request;

    @Override
    public Response toResponse(DataAlreadyExistException exception) {
        LOG.error("Invalid argument - data already exist for {}", request.getMethod(), exception);
        Response.ResponseBuilder responseBuilder = Response
                .status(Response.Status.CONFLICT);
        if (StringUtils.equalsAnyIgnoreCase("HEAD", request.getMethod())) {
            responseBuilder.header("Content-Length", 0);
        } else {
            String errorMessage = StringUtils.defaultIfBlank(exception.getMessage(), "Data entry or file already exists");
            responseBuilder
                    .entity(new ErrorResponse(errorMessage))
                    .type(MediaType.APPLICATION_JSON);
        }
        return responseBuilder.build();
    }

}
