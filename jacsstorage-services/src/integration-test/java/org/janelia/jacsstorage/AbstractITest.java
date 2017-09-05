package org.janelia.jacsstorage;

import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.junit.BeforeClass;

import java.util.Properties;

public abstract class AbstractITest {
    protected static Properties integrationTestsConfig;

    @BeforeClass
    public static void setUpTestsConfig() {
        integrationTestsConfig = new ApplicationConfigProvider()
                .fromFile("src/integration-test/resources/jacsstorage_test.properties")
                .fromEnvVar("JACSSTORAGE_CONFIG_TEST")
                .build();
    }

}
