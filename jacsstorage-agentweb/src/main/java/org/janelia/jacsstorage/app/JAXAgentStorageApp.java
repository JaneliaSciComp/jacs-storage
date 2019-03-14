package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.nonauthenticated.AgentConnectionResource;
import org.janelia.jacsstorage.rest.authenticated.AgentStorageResource;
import org.janelia.jacsstorage.rest.authenticated.PathBasedAgentStorageAuthenticatedResource;
import org.janelia.jacsstorage.rest.nonauthenticated.PathBasedAgentStorageUnauthenticatedResource;
import org.janelia.jacsstorage.rest.nonauthenticated.VolumeQuotaResource;
import org.janelia.jacsstorage.rest.nonauthenticated.VolumeStorageResource;
import org.janelia.jacsstorage.webdav.AgentWebdavResource;

import java.util.Set;

public class JAXAgentStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(
                AgentConnectionResource.class,
                AgentStorageResource.class,
                PathBasedAgentStorageAuthenticatedResource.class,
                PathBasedAgentStorageUnauthenticatedResource.class,
                VolumeStorageResource.class,
                VolumeQuotaResource.class,
                AgentWebdavResource.class
        );
    }
}
