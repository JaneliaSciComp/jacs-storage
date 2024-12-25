package org.janelia.jacsstorage.testrest;

import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.spi.CDIProvider;
import jakarta.ws.rs.core.Application;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.ext.cdi1x.internal.CdiServerComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.grizzly.GrizzlyTestContainerFactory;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.interceptors.TimedInterceptor;
import org.janelia.jacsstorage.rest.DataBundleStorageResource;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.interceptors.LoggerInterceptor;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public class AbstractCdiInjectedResourceTest extends JerseyTest {

    protected WeldContainer container;
    protected TestAgentStorageDependenciesProducer dependenciesProducer;

    @Override
    protected Application configure() {
        return new JAXAgentStorageApp();
    }

    @BeforeEach
    public void setup() {
        Assumptions.assumeTrue(Hk2InjectionManagerFactory.isImmediateStrategy());
    }

    @BeforeEach
    public void setUp() throws Exception {
        dependenciesProducer = new TestAgentStorageDependenciesProducer();
        CdiComponentProvider cdiComponentProvider = new CdiComponentProvider();
        Weld containerInit = new Weld()
                .disableDiscovery()
                .addExtensions(cdiComponentProvider)
                .addBeanClass(TestAgentStorageDependenciesProducer.class)
                .addBeanClass(DataBundleStorageResource.class)
                ;
        container = containerInit.initialize();
        super.setUp();
    }

    @Override
    protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
        return new GrizzlyTestContainerFactory();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
        super.tearDown();
    }

}
