package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.StorageAgentsResource;
import org.janelia.jacsstorage.rest.MasterStorageResource;
import org.janelia.jacsstorage.webdav.MasterWebdavResource;

import java.util.Set;

public class JAXMasterStorageApp extends AbstractJAXApp {
    @Override
    protected Set<Class<?>> getAppClasses() {
        return ImmutableSet.of(
                MasterStorageResource.class,
                StorageAgentsResource.class,
                MasterWebdavResource.class);
    }
}
