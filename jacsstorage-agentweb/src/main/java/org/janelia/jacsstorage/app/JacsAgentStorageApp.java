package org.janelia.jacsstorage.app;

import io.undertow.servlet.api.ListenerInfo;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;

/**
 * This is the agent storage application.
 */
public class JacsAgentStorageApp extends AbstractStorageApp {

    public static void main(String[] args) throws ServletException {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();
        app.start(args);
    }

    @Override
    protected String getJaxConfigName() {
        return JAXAgentStorageApp.class.getName();
    }

    @Override
    protected String getRestApiMapping() {
        return "/agent-api/*";
    }

    @Override
    protected ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

}
