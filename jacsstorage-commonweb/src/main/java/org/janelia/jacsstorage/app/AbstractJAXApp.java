package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;
import org.janelia.jacsstorage.rest.IllegalStateRequestHandler;
import org.janelia.jacsstorage.rest.InvalidJsonRequestHandler;
import org.janelia.jacsstorage.rest.JsonParseErrorRequestHandler;

import javax.ws.rs.core.Application;
import java.util.Set;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(ObjectMapperResolver.class, IllegalStateRequestHandler.class, InvalidJsonRequestHandler.class, JsonParseErrorRequestHandler.class)
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
