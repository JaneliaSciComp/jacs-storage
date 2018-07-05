package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.ContentStorageResource;
import org.janelia.jacsstorage.rest.MasterStatusResource;
import org.janelia.jacsstorage.rest.MasterStorageQuotaResource;
import org.janelia.jacsstorage.rest.StorageAgentsResource;
import org.janelia.jacsstorage.rest.MasterStorageResource;
import org.janelia.jacsstorage.rest.StorageVolumesResource;
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
