package org.janelia.jacsstorage.app.undertow;

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
import io.undertow.predicate.Predicates;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.resource.PathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.ServletInfo;
import io.undertow.util.HttpString;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;
import org.janelia.jacsstorage.app.AppArgs;
import org.janelia.jacsstorage.app.ContainerInitializer;
import org.janelia.jacsstorage.app.ContextPathBuilder;
import org.jboss.weld.environment.servlet.Listener;
import org.jboss.weld.module.web.servlet.WeldInitialListener;
import org.jboss.weld.module.web.servlet.WeldTerminalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.core.Application;
import java.nio.file.Paths;

import static io.undertow.Handlers.resource;
import static io.undertow.servlet.Servlets.servlet;

public class UndertowContainerInitializer implements ContainerInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowContainerInitializer.class);

    private final String applicationId;
    private final String restApiContext;
    private final String restApiVersion;
    private final String[] excludedPathsFromAccessLog;

    private Undertow server;

    public UndertowContainerInitializer(String applicationId,
                                        String restApiContext,
                                        String restApiVersion,
                                        String[] excludedPathsFromAccessLog) {
        this.applicationId = applicationId;
        this.restApiContext = restApiContext;
        this.restApiVersion = restApiVersion;
        this.excludedPathsFromAccessLog = excludedPathsFromAccessLog;
    }

    @Override
    public void initialize(Application application, AppArgs appArgs) throws ServletException {
        String contextPath = new ContextPathBuilder()
                .path(appArgs.baseContextPath)
                .path(restApiContext)
                .path(restApiVersion)
                .build();
        String docsContextPath = "/docs";
        ServletInfo restApiServlet =
                Servlets.servlet("restApiServlet", ServletContainer.class)
                        .setLoadOnStartup(1)
                        .setAsyncSupported(true)
                        .setEnabled(true)
                        .addInitParam(ServletProperties.JAXRS_APPLICATION_CLASS, application.getClass().getName())
                        .addInitParam("jersey.config.server.wadl.disableWadl", "true")
                        .addMapping("/*")
                ;

        String basepath = "http://" + appArgs.host + ":" + appArgs.portNumber + contextPath;
        ServletInfo swaggerDocsServlet =
                servlet("swaggerDocsServlet", JerseyJaxrsConfig.class)
                        .setLoadOnStartup(2)
                        .addInitParam("api.version", restApiVersion)
                        .addInitParam("swagger.api.basepath", basepath);

        DeploymentInfo servletBuilder =
                Servlets.deployment()
                        .setClassLoader(this.getClass().getClassLoader())
                        .setContextPath(contextPath)
                        .setDeploymentName(appArgs.deployment)
                        .setEagerFilterInit(true)
                        .addListener(Servlets.listener(WeldInitialListener.class))
                        .addListener(Servlets.listener(Listener.class))
                        .addListener(Servlets.listener(WeldTerminalListener.class))
                        .addServlets(restApiServlet, swaggerDocsServlet)
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
                new Slf4jAccessLogReceiver(LoggerFactory.getLogger(application.getClass())),
                "ignored",
                new JoinedExchangeAttribute(new ExchangeAttribute[] {
                        RemoteHostAttribute.INSTANCE, // <RemoteIP>
                        RemoteUserAttribute.INSTANCE, // <RemoteUser>
                        new ConstantExchangeAttribute(applicationId), // <Application-Id>
                        DateTimeAttribute.INSTANCE, // <timestamp>
                        RequestMethodAttribute.INSTANCE, // <HttpVerb>
                        RequestPathAttribute.INSTANCE, // <RequestPath>
                        new NameValueAttribute("location", new ResponseHeaderAttribute(new HttpString("Location"))), // location=<ResponseLocation>
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

    @Override
    public void start() {
        server.start();
    }

    private Predicate getAccessLogFilter() {
        return Predicates.not(
                Predicates.prefixes(excludedPathsFromAccessLog)
        );
    }
}