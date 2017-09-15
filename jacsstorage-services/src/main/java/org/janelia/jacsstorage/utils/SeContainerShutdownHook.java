package org.janelia.jacsstorage.utils;

import javax.enterprise.inject.se.SeContainer;

public class SeContainerShutdownHook extends Thread {
    private final SeContainer seContainer;


    public SeContainerShutdownHook(final SeContainer seContainer) {
        this.seContainer = seContainer;
    }

    public void run() {
        seContainer.close();
    }
}
