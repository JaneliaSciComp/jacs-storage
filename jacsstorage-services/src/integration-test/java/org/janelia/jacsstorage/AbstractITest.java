package org.janelia.jacsstorage;

import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.junit.BeforeClass;

public abstract class AbstractITest {
    protected static ApplicationConfig integrationTestsConfig;

    @BeforeClass
    public static void setUpTestsConfig() {
        integrationTestsConfig = new ApplicationConfigProvider()
                .fromFile("src/integration-test/resources/jacsstorage_test.properties")
                .fromEnvVar("JACSSTORAGE_CONFIG_TEST")
                .build();
    }

}
