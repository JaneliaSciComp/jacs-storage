package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.filter.CORSResponseFilter;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import java.util.Map;

/**
 * This is the bootstrap application for JACS services.
 */
public abstract class AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageApp.class);

    private Undertow server;

    protected static class AppArgs {
        @Parameter(names = "-b", description = "Binding IP", required = false)
        protected String host = "localhost";
        @Parameter(names = "-p", description = "Listener port number", required = false)
        protected int portNumber = 8080;
        @Parameter(names = "-name", description = "Deployment name", required = false)
        protected String deployment = "jacsstorage";
        @Parameter(names = "-context-path", description = "Base context path", required = false)
        protected String baseContextPath = "jacsstorage";
        @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
        protected boolean displayUsage = false;
        @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
        private Map<String, String> applicationArgs = ApplicationConfigProvider.applicationArgs();
    }

    protected void start(AppArgs appArgs) {
        try {
            initializeApp(appArgs);
            run();
        } catch (ServletException e) {
            LOG.error("Error starting the application", e);
        }
    }

    protected void initializeApp(AppArgs appArgs) throws ServletException {
        String contextPath = "/" + appArgs.baseContextPath;
        ServletInfo restApiServlet =
                Servlets.servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .setAsyncSupported(true)
                        .setEnabled(true)
                        .addInitParam(ServletProperties.JAXRS_APPLICATION_CLASS, getJaxConfigName())
                        .addMapping(getRestApiMapping())
                ;
        DeploymentInfo servletBuilder =
                Servlets.deployment()
                        .setClassLoader(this.getClass().getClassLoader())
                        .setContextPath(contextPath)
                        .setDeploymentName(appArgs.deployment)
                        .setEagerFilterInit(true)
                        .addListener(Servlets.listener(WeldInitialListener.class))
                        .addListener(Servlets.listener(Listener.class))
                        .addListeners(getAppListeners())
                        .addListener(Servlets.listener(WeldTerminalListener.class))
                        .addServlets(restApiServlet)
                ;

        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(servletBuilder);
        deploymentManager.deploy();
        HttpHandler restApiHttpHandler = deploymentManager.start();

        PathHandler storageHandler = Handlers.path(Handlers.redirect(contextPath))
                .addPrefixPath(contextPath, restApiHttpHandler);

        LOG.info("Start JACS storage listener on {}:{}", appArgs.host, appArgs.portNumber);
        server = Undertow
                    .builder()
                    .addHttpListener(appArgs.portNumber, appArgs.host)
                    .setHandler(storageHandler)
                    .build();
    }

    private void run() {
        server.start();
    }

    protected abstract String getJaxConfigName();

    protected abstract String getRestApiMapping();

    protected abstract ListenerInfo[] getAppListeners();
}
