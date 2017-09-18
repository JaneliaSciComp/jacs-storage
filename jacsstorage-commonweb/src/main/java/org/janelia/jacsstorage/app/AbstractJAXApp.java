package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;

import javax.ws.rs.core.Application;
import java.util.Set;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .addAll(getAppClasses())
                .build();
    }

    @Override
    public Set<Object> getSingletons() {
        return ImmutableSet.builder()
                .add(new ObjectMapperResolver(ObjectMapperFactory.instance().getDefaultObjectMapper()))
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();
}
