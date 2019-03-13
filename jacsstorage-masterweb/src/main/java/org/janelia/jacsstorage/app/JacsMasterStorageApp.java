package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.core.Application;

/**
 * This is the master storage application.
 */
public class JacsMasterStorageApp extends AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(JacsMasterStorageApp.class);
    private static final String DEFAULT_APP_ID = "JacsStorageMaster";

    public static void main(String[] args) {
        try {
            final AppArgs appArgs = parseAppArgs(args, new AppArgs());
            if (appArgs.displayUsage) {
                displayAppUsage(appArgs);
                return;
            }
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();
            JacsMasterStorageApp app = container.select(JacsMasterStorageApp.class).get();
            app.start(appArgs);
        } catch (Throwable e) {
            LOG.error("Error starting application", e);
        }
    }

    @Override
    String getApplicationId(AppArgs appArgs) {
        if (StringUtils.isBlank(appArgs.applicationId)) {
            return DEFAULT_APP_ID;
        } else {
            return appArgs.applicationId;
        }
    }

    @Override
    Application getJaxApplication() {
        return new JAXMasterStorageApp();
    }

    @Override
    String getRestApiContext() {
        return "master_api";
    }

    @Override
    String[] getPathsExcludedFromAccessLog() {
        return new String[] {
                "/agents/url"
        };
    }
}
