package org.janelia.jacsstorage;

import com.google.common.collect.ImmutableMap;

import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.junit.BeforeClass;

public abstract class AbstractITest {
    protected static ApplicationConfig integrationTestsConfig;

    @BeforeClass
    public static void setUpTestsConfig() {
        integrationTestsConfig = new ApplicationConfigProvider()
                .fromResource("/jacsstorage.properties")
                .fromMap(ImmutableMap.of(
                        "user.name", System.getProperty("user.name"),
                        "MongoDB.ConnectionURL", "mongodb://localhost:27017",
                        "MongoDB.Database", "${user.name}_jacsstorage_test"
                ))
                .fromFile("src/integrationTest/resources/jacsstorage_test.properties")
                .fromEnvVar("JACSSTORAGE_CONFIG_TEST")
                .build();
    }

}
