package org.janelia.jacsstorage.testrest;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.test.JerseyTest;
import org.janelia.jacsstorage.app.JAXMasterStorageApp;
import org.janelia.jacsstorage.rest.MasterStorageQuotaResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class AbstractCdiInjectedResourceTest extends JerseyTest {

    private SeContainer container;
    protected TestMasterStorageDependenciesProducer dependenciesProducer;

    @Override
    protected Application configure() {
        return new JAXMasterStorageApp();
    }

    @BeforeEach
    public void setUp() throws Exception {
        dependenciesProducer = new TestMasterStorageDependenciesProducer();
        CdiComponentProvider cdiComponentProvider = new CdiComponentProvider();
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addExtensions(cdiComponentProvider)
                .addBeanClasses(
                        TestMasterStorageDependenciesProducer.class,
                        MasterStorageQuotaResource.class
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
