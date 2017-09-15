package org.janelia.jacsstorage.app;

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
        app.start(args);
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
