package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import io.undertow.servlet.api.ListenerInfo;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;

/**
 * This is the master storage application.
 */
public class JacsMasterStorageApp extends AbstractStorageApp {

    public static void main(String[] args) throws ServletException {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        JacsMasterStorageApp app = container.select(JacsMasterStorageApp.class).get();
        final AppArgs appArgs = new AppArgs();
        JCommander cmdline = new JCommander(appArgs);
        cmdline.parse(args);
        if (appArgs.displayUsage) {
            cmdline.usage();
            return;
        }
        app.start(appArgs);
    }

    @Override
    protected String getJaxConfigName() {
        return JAXMasterStorageApp.class.getName();
    }

    @Override
    protected String getRestApiMapping() {
        return "/master-api/*";
    }

    @Override
    protected ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

}
