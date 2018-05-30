package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import io.swagger.jersey.config.JerseyJaxrsConfig;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.attribute.BytesSentAttribute;
import io.undertow.attribute.ConstantExchangeAttribute;
import io.undertow.attribute.DateTimeAttribute;
import io.undertow.attribute.ExchangeAttribute;
import io.undertow.attribute.RemoteHostAttribute;
import io.undertow.attribute.RemoteUserAttribute;
import io.undertow.attribute.RequestMethodAttribute;
import io.undertow.attribute.RequestPathAttribute;
import io.undertow.attribute.ResponseCodeAttribute;
import io.undertow.attribute.ResponseHeaderAttribute;
import io.undertow.predicate.Predicate;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ListenerInfo;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.util.HttpString;
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
        @Parameter(names = "-nio", description = "Number of IO threads", required = false)
        private int nIOThreads = 64;
        @Parameter(names = "-nworkers", description = "Number of worker threads", required = false)
        private int nWorkers = 64 * 8;
        @Parameter(names = "-appId", description = "application ID")
        protected String applicationId;
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

        HttpHandler storageHandler = new AccessLogHandler(
                Handlers.path(
                        Handlers.redirect(docsContextPath))
                        .addPrefixPath(docsContextPath, staticHandler)
                        .addPrefixPath(contextPath, restApiHttpHandler),
                new Slf4jAccessLogReceiver(LoggerFactory.getLogger(getJaxConfigName())),
                "ignored",
                new JoinedExchangeAttribute(new ExchangeAttribute[] {
                        RemoteHostAttribute.INSTANCE, // <RemoteIP>
                        RemoteUserAttribute.INSTANCE, // <RemoteUser>
                        new ConstantExchangeAttribute(getApplicationId(appArgs)), // <Application-Id>
                        DateTimeAttribute.INSTANCE, // <timestamp>
                        RequestMethodAttribute.INSTANCE, // <HttpVerb>
                        RequestPathAttribute.INSTANCE, // <RequestPath>
                        new NameValueAttribute("location_header", new ResponseHeaderAttribute(new HttpString("Location"))), // location=<ResponseLocation>
                        new NameValueAttribute("status", ResponseCodeAttribute.INSTANCE), // status=<ResponseStatus>
                        new NameValueAttribute("response_bytes", new BytesSentAttribute(false)), // response_bytes=<ResponseBytes>
                        new NameValueAttribute("rt", new ResponseTimeAttribute()), // rt=<ResponseTime>
                        new NameValueAttribute("tp", new ThroughputAttribute()) // tp=<Throughput>
                }, " "),
                getAccessLogFilter()
        );

        LOG.info("Start JACS storage listener on {}:{}", appArgs.host, appArgs.portNumber);
        server = Undertow
                    .builder()
                    .addHttpListener(appArgs.portNumber, appArgs.host)
                    .setIoThreads(appArgs.nIOThreads)
                    .setServerOption(UndertowOptions.RECORD_REQUEST_START_TIME, true)
                    .setWorkerThreads(appArgs.nWorkers)
                    .setHandler(storageHandler)
                    .build();
    }

    private void run() {
        server.start();
    }

    abstract String getApplicationId(AppArgs appArgs);

    abstract String getJaxConfigName();

    abstract String getRestApi(AppArgs appArgs);

    abstract ListenerInfo[] getAppListeners();

    abstract Predicate getAccessLogFilter();
}
