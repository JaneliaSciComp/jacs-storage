package org.janelia.jacsstorage.provider;

import jakarta.inject.Inject;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.datarequest.NumberSerializerModule;

public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    @Inject
    public ObjectMapperResolver(ObjectMapperFactory mapperFactory) {
        this.mapper = mapperFactory.newObjectMapper()
                .registerModule(new NumberSerializerModule())
        ;
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }
}
