package org.janelia.jacsstorage.service;

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
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        StorageServerLauncher storageServerLauncher = container.select(StorageServerLauncher.class).get();
        storageServerLauncher.agentListener.startServer();
    }
}
