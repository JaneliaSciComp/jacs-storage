package org.janelia.jacsstorage.app;

import javax.servlet.ServletException;
import javax.ws.rs.core.Application;

public interface ContainerInitializer {
    void initialize(Application application, AppArgs appArgs) throws ServletException;
    void start();
    void stop();
}
