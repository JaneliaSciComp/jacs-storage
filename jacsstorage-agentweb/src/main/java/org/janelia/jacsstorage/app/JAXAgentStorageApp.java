package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.AgentConnectionResource;
import org.janelia.jacsstorage.rest.AgentStorageResource;
import org.janelia.jacsstorage.rest.PathBasedAgentStorageResource;
import org.janelia.jacsstorage.webdav.AgentWebdavResource;

import java.util.Set;

public class JAXAgentStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(
                AgentConnectionResource.class,
                AgentStorageResource.class,
                PathBasedAgentStorageResource.class,
                AgentWebdavResource.class
        );
    }
}
