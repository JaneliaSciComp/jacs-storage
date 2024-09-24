package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.*;
import org.janelia.jacsstorage.webdav.AgentWebdavResource;

import java.util.Set;

public class JAXAgentStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(
                AgentConnectionResource.class,
                DeprecateAgentStorageResource.class,
                PathBasedAgentStorageResource.class,
                VolumeStorageResource.class,
                VolumeQuotaResource.class,
                AgentWebdavResource.class,
                N5StorageResource.class
        );
    }
}
