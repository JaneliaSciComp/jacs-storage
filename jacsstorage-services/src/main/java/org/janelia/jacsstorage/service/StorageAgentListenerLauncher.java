package org.janelia.jacsstorage.service;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;

public class StorageAgentListenerLauncher {

    private final StorageAgentListener agentListener;

    @Inject
    public StorageAgentListenerLauncher(StorageAgentListener agentListener) {
        this.agentListener = agentListener;
    }

    public static void main(String[] args) throws Exception {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        StorageAgentListenerLauncher storageAgentListenerLauncher = container.select(StorageAgentListenerLauncher.class).get();
        storageAgentListenerLauncher.agentListener.open();
        storageAgentListenerLauncher.agentListener.startServer();
    }
}
