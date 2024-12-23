package org.janelia.jacsstorage.app;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the master storage application.
 */
public class JacsMasterStorageApp extends AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(JacsMasterStorageApp.class);
    private static final String DEFAULT_APP_ID = "JacsStorageMaster";

    public static void main(String[] args) {
        final AppArgs appArgs = parseAppArgs(args, new AppArgs());
        if (appArgs.displayUsage) {
            displayAppUsage(appArgs);
            return;
        }
        try {
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();

            JacsMasterStorageApp app = container.select(JacsMasterStorageApp.class).get();
            ApplicationConfig appConfig = container.select(ApplicationConfig.class, new ApplicationProperties() {
                @Override
                public Class<ApplicationProperties> annotationType() {
                    return ApplicationProperties.class;
                }
            }).get();
            LOG.info("Start master app with {}", appConfig);
            app.start(appArgs, appConfig);
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
