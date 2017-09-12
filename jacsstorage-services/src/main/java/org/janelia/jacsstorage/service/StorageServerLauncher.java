package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.ApplicationProducer;
import org.janelia.jacsstorage.dao.Dao;
import org.janelia.jacsstorage.io.DataBundleIOProvider;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;

public class StorageServerLauncher {

    private final StorageAgentListener agentListener;

    @Inject
    public StorageServerLauncher(StorageAgentListener agentListener) {
        this.agentListener = agentListener;
    }

    public static void main(String[] args) throws Exception {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance()
                .disableDiscovery()
                .addPackages(true,
                        Dao.class,
                        StorageServerLauncher.class,
                        DataBundleIOProvider.class,
                        ApplicationProducer.class
                )
                ;
        try (SeContainer container = containerInit.initialize()) {
            StorageServerLauncher storageServerLauncher = container.select(StorageServerLauncher.class).get();
            storageServerLauncher.agentListener.startServer();
        }
    }
}
