package org.janelia.jacsstorage.cdi;

import org.janelia.jacsstorage.service.StorageAgentManager;
import org.janelia.jacsstorage.service.StorageAgentManagerImpl;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class ServicesProducer {

    @ApplicationScoped
    @Produces
    public StorageAgentManager storageAgentManager(Logger logger) {
        return new StorageAgentManagerImpl(logger);
    }

}
