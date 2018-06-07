package org.janelia.jacsstorage.app;

import org.janelia.jacsstorage.app.undertow.UndertowContainerInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Application;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageApp.class);

    protected void start(AppArgs appArgs) {
        ContainerInitializer containerInitializer = new UndertowContainerInitializer(
                getApplicationId(appArgs),
                getRestApiContext(),
                getApiVersion(),
                getPathsExcludedFromAccessLog()
        );
        try {
            containerInitializer.initialize(this.getJaxApplication(), appArgs);
            containerInitializer.start();
        } catch (Exception e) {
            LOG.error("Error starting the application", e);
        }
    }

    String getApiVersion() {
        return "v1";
    }

    abstract String getApplicationId(AppArgs appArgs);

    abstract Application getJaxApplication();

    abstract String getRestApiContext();

    abstract String[] getPathsExcludedFromAccessLog();
}
