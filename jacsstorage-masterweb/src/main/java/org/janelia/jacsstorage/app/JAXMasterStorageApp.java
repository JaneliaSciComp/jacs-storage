package org.janelia.jacsstorage.app;

import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.rest.StorageResource;

import javax.ws.rs.core.Application;
import java.util.Set;

public class JAXMasterStorageApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.of(StorageResource.class);
    }
}
