package org.janelia.jacsstorage.app;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.AgentConnectionResource;
import org.janelia.jacsstorage.rest.DataBundleStorageResource;
import org.janelia.jacsstorage.rest.N5StorageResource;
import org.janelia.jacsstorage.rest.PathBasedAgentStorageResource;
import org.janelia.jacsstorage.rest.VolumeQuotaResource;
import org.janelia.jacsstorage.rest.VolumeStorageResource;
import org.janelia.jacsstorage.webdav.AgentWebdavResource;

public class JAXAgentStorageApp extends AbstractJAXApp {

    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(AgentConnectionResource.class)
                .add(DataBundleStorageResource.class)
                .add(PathBasedAgentStorageResource.class)
                .add(VolumeStorageResource.class)
                .add(VolumeQuotaResource.class)
                .add(AgentWebdavResource.class)
                .add(N5StorageResource.class)
                .build();
    }

}
