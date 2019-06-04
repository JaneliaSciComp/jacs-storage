package org.janelia.jacsstorage.app;

import javax.annotation.PreDestroy;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeShutdown;
import javax.ws.rs.core.Application;

import com.beust.jcommander.JCommander;

import org.janelia.jacsstorage.app.undertow.UndertowAppContainer;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageApp.class);

    static <A extends AppArgs> A parseAppArgs(String[] args, A appArgs) {
        JCommander cmdline = new JCommander(appArgs);
        cmdline.parse(args);
        // update the dynamic config
        ApplicationConfigProvider.setAppDynamicArgs(appArgs.appDynamicConfig);
        return appArgs;
    }

    static <A extends AppArgs> void displayAppUsage(A appArgs) {
        displayAppUsage(appArgs, new StringBuilder());
    }

    static <A extends AppArgs> void displayAppUsage(A appArgs, StringBuilder output) {
        JCommander cmdline = new JCommander(appArgs);
        cmdline.usage(output);
    }

    private AppContainer appContainer;

    protected void start(AppArgs appArgs, ApplicationConfig applicationConfig) {
        appContainer = new UndertowAppContainer(
                getApplicationId(appArgs),
                getRestApiContext(),
                getApiVersion(),
                getPathsExcludedFromAccessLog(),
                applicationConfig
        );
        try {
            appContainer.initialize(this.getJaxApplication(), appArgs);
            appContainer.start();
        } catch (Exception e) {
            LOG.error("Error starting the application", e);
        }
    }

    @PreDestroy
    public void shutdownApp() {
        if (appContainer != null) {
            LOG.info("Stopping the container");
            appContainer.stop();
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
