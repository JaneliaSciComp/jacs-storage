package org.janelia.jacsstorage.testrest;

import org.glassfish.jersey.ext.cdi1x.internal.CdiComponentProvider;
import org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.enterprise.inject.spi.Extension;

import static org.mockito.Mockito.spy;

public class AbstractCdiInjectedResourceTest extends JerseyTest {

    protected SeContainer container;

    @Before
    public void setupPreconditionCheck() {
        Assume.assumeTrue(Hk2InjectionManagerFactory.isImmediateStrategy());
    }

    @Before
    public void setUp() throws Exception {
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .disableDiscovery()
                .addExtensions((Extension) new CdiComponentProvider())
                .addBeanClasses(getTestBeanProviders())
                ;
        container = spy(containerInit.initialize());
        super.setUp();
    }

    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[0];
    }

    @After
    public void tearDown() throws Exception {
        container.close();
        super.tearDown();
    }

}
