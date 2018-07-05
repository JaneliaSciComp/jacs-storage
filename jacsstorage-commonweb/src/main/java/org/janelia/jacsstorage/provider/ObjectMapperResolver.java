package org.janelia.jacsstorage.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.datarequest.NumberSerializerModule;

import javax.inject.Inject;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
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
