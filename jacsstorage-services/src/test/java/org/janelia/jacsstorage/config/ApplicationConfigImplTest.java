package org.janelia.jacsstorage.config;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ApplicationConfigImplTest {

    @Test
    public void expandedStringProperty() {
        ApplicationConfig applicationConfig = new ApplicationConfigImpl();
        applicationConfig.putAll(ImmutableMap.of(
                "A", "12345",
                "B", "${A}67890",
                "C", "${B} plus more"
        ));
        assertEquals("1234567890 plus more", applicationConfig.getStringPropertyValue("C"));
    }
}
