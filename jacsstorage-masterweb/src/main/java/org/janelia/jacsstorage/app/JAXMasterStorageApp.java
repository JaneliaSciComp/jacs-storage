package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.nonauthenticated.ContentStorageResource;
import org.janelia.jacsstorage.rest.nonauthenticated.MasterStatusResource;
import org.janelia.jacsstorage.rest.nonauthenticated.MasterStorageQuotaResource;
import org.janelia.jacsstorage.rest.nonauthenticated.StorageAgentsResource;
import org.janelia.jacsstorage.rest.authenticated.MasterStorageResource;
import org.janelia.jacsstorage.rest.authenticated.StorageVolumesResource;
import org.janelia.jacsstorage.webdav.MasterWebdavResource;

import java.util.Set;

public class JAXMasterStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(
                ContentStorageResource.class,
                MasterStorageResource.class,
                MasterStorageQuotaResource.class,
                MasterStatusResource.class,
                StorageAgentsResource.class,
                StorageVolumesResource.class,
                MasterWebdavResource.class
        );
    }
}
