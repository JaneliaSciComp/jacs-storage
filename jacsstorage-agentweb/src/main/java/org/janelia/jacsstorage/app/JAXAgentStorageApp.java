package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.AgentStatusResource;

import java.util.Set;

public class JAXAgentStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(AgentStatusResource.class);
    }
}
