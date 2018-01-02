package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.swagger.jersey.config.JerseyJaxrsConfig;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import java.nio.file.Paths;
import java.util.Map;

import static io.undertow.Handlers.resource;
import static io.undertow.servlet.Servlets.servlet;

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
        protected String baseContextPath = "/jacsstorage";
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

    String getApiVersion() {
        return "v1";
    }

    void initializeApp(AppArgs appArgs) throws ServletException {
        String contextPath = getRestApi(appArgs);
        String docsContextPath = "/docs";
        ServletInfo restApiServlet =
                Servlets.servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .setAsyncSupported(true)
                        .setEnabled(true)
                        .addInitParam(ServletProperties.JAXRS_APPLICATION_CLASS, getJaxConfigName())
                        .addInitParam("jersey.config.server.wadl.disableWadl", "true")
                        .addMapping("/*")
                ;

        String basepath = "http://" + appArgs.host + ":" + appArgs.portNumber + contextPath;
        ServletInfo swaggerServlet =
                servlet("swaggerServlet", JerseyJaxrsConfig.class)
                        .setLoadOnStartup(2)
                        .addInitParam("api.version", getApiVersion())
                        .addInitParam("swagger.api.basepath", basepath);

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
                        .addServlets(restApiServlet, swaggerServlet)
                ;

        LOG.info("Deploy REST API at {}", basepath);
        DeploymentManager deploymentManager = Servlets.defaultContainer().addDeployment(servletBuilder);
        deploymentManager.deploy();
        HttpHandler restApiHttpHandler = deploymentManager.start();

        // Static handler for Swagger resources
        ResourceHandler staticHandler =
                resource(new PathResourceManager(Paths.get("swagger-webapp"), 100));

        PathHandler storageHandler = Handlers.path(
                Handlers.redirect(docsContextPath))
                .addPrefixPath(docsContextPath, staticHandler)
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

    abstract String getJaxConfigName();

    abstract String getRestApi(AppArgs appArgs);

    abstract ListenerInfo[] getAppListeners();
}
