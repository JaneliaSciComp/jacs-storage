package org.janelia.jacsstorage.testrest;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.test.JerseyTest;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.rest.DataBundleStorageResource;
import org.janelia.jacsstorage.rest.PathBasedAgentStorageResource;
import org.janelia.jacsstorage.rest.VolumeQuotaResource;
import org.janelia.jacsstorage.rest.VolumeStorageResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public class AbstractCdiInjectedResourceTest extends JerseyTest {

    private SeContainer container;
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
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addExtensions(cdiComponentProvider)
                .addBeanClasses(
                        TestAgentStorageDependenciesProducer.class,
                        DataBundleStorageResource.class,
                        PathBasedAgentStorageResource.class,
                        VolumeQuotaResource.class,
                        VolumeStorageResource.class
                );
        container = containerInit.initialize();
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
        super.tearDown();
    }

}
