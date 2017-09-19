package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;

import javax.ws.rs.core.Application;
import java.util.Set;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(ObjectMapperResolver.class)
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
