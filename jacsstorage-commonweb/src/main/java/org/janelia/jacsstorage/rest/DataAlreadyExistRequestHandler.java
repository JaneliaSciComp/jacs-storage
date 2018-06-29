package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.DataAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DataAlreadyExistRequestHandler implements ExceptionMapper<DataAlreadyExistException> {
    private static final Logger LOG = LoggerFactory.getLogger(DataAlreadyExistRequestHandler.class);

    @Override
    public Response toResponse(DataAlreadyExistException exception) {
        LOG.error("Invalid argument response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Data entry or file already exists";
        }
        return Response
                .status(Response.Status.CONFLICT)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}
