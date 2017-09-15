package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.utils.SeContainerShutdownHook;

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
        Runtime.getRuntime().addShutdownHook(new SeContainerShutdownHook(container)); // add the SE shutdown hook
        storageServerLauncher.agentListener.startServer();
    }
}
