package org.janelia.jacsstorage.app;

import javax.servlet.ServletException;

public interface ContainerInitializer {
    void initialize(AppArgs appArgs) throws ServletException;
    void start();
}
