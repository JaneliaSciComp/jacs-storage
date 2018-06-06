package org.janelia.jacsstorage.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageApp.class);

    protected void start(AppArgs appArgs) {
        ContainerInitializer containerInitializer = new UndertowContainerInitializer(
                getApplicationId(appArgs),
                getJaxConfigName(),
                getRestApiContext(),
                getApiVersion(),
                getPathsExcludedFromAccessLog()
        );
        try {
            containerInitializer.initialize(appArgs);
            containerInitializer.start();
        } catch (Exception e) {
            LOG.error("Error starting the application", e);
        }
    }

    String getApiVersion() {
        return "v1";
    }

    abstract String getApplicationId(AppArgs appArgs);

    abstract String getJaxConfigName();

    abstract String getRestApiContext();

    abstract String[] getPathsExcludedFromAccessLog();
}
