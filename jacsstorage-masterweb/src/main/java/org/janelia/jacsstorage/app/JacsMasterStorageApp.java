package org.janelia.jacsstorage.app;

import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.ListenerInfo;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.janelia.jacsstorage.cdi.WebAppProducer;
import org.janelia.jacsstorage.rest.StorageResource;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.listener;

/**
 * This is the master storage application.
 */
public class JacsMasterStorageApp extends AbstractStorageApp {

    public static void main(String[] args) throws ServletException {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance()
                .addPackages(true,
                        JacsMasterStorageApp.class,
                        WebAppProducer.class,
                        StorageResource.class
                )
                ;
        SeContainer container = containerInit.initialize();
        JacsMasterStorageApp app = container.select(JacsMasterStorageApp.class).get();
        app.start(args);
    }

    @Override
    protected String getJaxConfigName() {
        String configName = JAXMasterStorageApp.class.getName();
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! CONFIG " + configName);
        return configName;
    }

    @Override
    protected String getRestApiMapping() {
        return "/master-api/*";
    }

    @Override
    protected ListenerInfo[] getListeners() {
        return new ListenerInfo[] {
                Servlets.listener(WeldInitialListener.class),
                Servlets.listener(WeldTerminalListener.class),
                Servlets.listener(Listener.class)
        };
    }

    @ApplicationScoped
    @Produces
    protected ServletContainer createServletContainer() {
        final ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(StorageResource.class);
        return new ServletContainer(resourceConfig);
    }
}
