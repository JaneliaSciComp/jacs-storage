package org.janelia.jacsstorage.cdi;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

public class WebAppProducer {
    @Produces
    @ApplicationScoped
    public ObjectMapperFactory objectMapperFactory() {
        return ObjectMapperFactory.instance();
    }

    @Produces
    public ObjectMapper objectMapper() {
        return ObjectMapperFactory.instance().getDefaultObjectMapper();
    }
}
