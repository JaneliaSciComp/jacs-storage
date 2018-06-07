package org.janelia.jacsstorage.app.grizzly;

import io.swagger.jersey.config.JerseyJaxrsConfig;
import org.glassfish.grizzly.http.server.HttpServer;

import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.http.server.StaticHttpHandler;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.grizzly.servlet.DefaultServlet;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.servlet.ServletProperties;
import org.janelia.jacsstorage.app.AppArgs;
import org.janelia.jacsstorage.app.ContainerInitializer;
import org.janelia.jacsstorage.app.ContextPathBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.net.URI;

public class GrizzlyContainerInitializer implements ContainerInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(GrizzlyContainerInitializer.class);

    private final String applicationId;
    private final String restApiContext;
    private final String restApiVersion;
    private final String[] excludedPathsFromAccessLog;

    private HttpServer server;

    public GrizzlyContainerInitializer(String applicationId,
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

        String basepath = "http://" + appArgs.host + ":" + appArgs.portNumber;

        URI serverURI = URI.create(basepath);
        LOG.info("Server URI: {}", serverURI);
        server = GrizzlyHttpServerFactory.createHttpServer(serverURI, false);

        WebappContext restApiContext = new WebappContext("RestApiContext", contextPath);
        ServletRegistration restApiRegistration = restApiContext.addServlet("RestApiServlet", ServletContainer.class);
        restApiRegistration.setLoadOnStartup(1);
        restApiRegistration.setInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, application.getClass().getName());
        restApiRegistration.setInitParameter("jersey.config.server.wadl.disableWadl", "true");
        restApiRegistration.addMapping("/*");
        restApiContext.deploy(server);

        String swaggerBasePath = "http://" + appArgs.host + ":" + appArgs.portNumber + contextPath;
        StaticHttpHandler staticHttpHandler = new StaticHttpHandler("swagger-webapp");
        ServerConfiguration serverConfiguration = server.getServerConfiguration();
        serverConfiguration.addHttpHandler(staticHttpHandler);

        WebappContext swaggerDocsContext = new WebappContext("SwaggerDocsContext", docsContextPath);
        ServletRegistration swaggerDocsRegistration = swaggerDocsContext.addServlet("SwaggerDocsServlet", JerseyJaxrsConfig.class);
        swaggerDocsRegistration.setLoadOnStartup(2);
        swaggerDocsRegistration.setInitParameter("api.version", restApiVersion);
        swaggerDocsRegistration.setInitParameter("swagger.api.basepath", swaggerBasePath);
        swaggerDocsRegistration.addMapping(docsContextPath);

        swaggerDocsContext.addServlet("SwaggerDocsContentServlet", new DefaultServlet(new StaticHttpHandler("swagger-webapp")) {});
        swaggerDocsContext.deploy(server);

        new AccessLogBuilder(applicationId + "-accesslog.log").instrument(server.getServerConfiguration());
        LOG.info("Start JACS storage listener on {}:{}", appArgs.host, appArgs.portNumber);

    }

    @Override
    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            LOG.error("Error running the container");
        }
    }

}
