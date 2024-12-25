package org.janelia.jacsstorage.testrest;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.Mockito.spy;

public class AbstractCdiInjectedResourceTest extends JerseyTest {

    private SeContainer container;

    @BeforeEach
    public void setUp() throws Exception {
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addBeanClasses(getTestBeanProviders())
                ;
        container = spy(containerInit.initialize());
        super.setUp();
    }

    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[0];
    }

    @AfterEach
    public void tearDown() throws Exception {
        container.close();
        super.tearDown();
    }

}
