package org.janelia.jacsstorage.cdi;

import com.fasterxml.jackson.databind. ObjectMapper;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.TimebasedIdGenerator;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.io.IOException;

@ApplicationScoped
public class ApplicationProducer {

    @Produces
    public ObjectMapper objectMapper(ObjectMapperFactory objectMapperFactory) {
        return objectMapperFactory.getDefaultObjectMapper();
    }

    @Produces
    @ApplicationScoped
    public IdGenerator idGenerator(@PropertyValue(name = "TimebasedIdentifierGenerator.DeploymentContext") Integer deploymentContext) {
        return new TimebasedIdGenerator(deploymentContext);
    }

    @Produces
    @PropertyValue(name = "")
    public String stringPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getStringPropertyValue(property.name());
    }

    @Produces
    @PropertyValue(name = "")
    public Integer integerPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        return applicationConfig.getIntegerPropertyValue(property.name());
    }

    @ApplicationScoped
    @ApplicationProperties
    @Produces
    public ApplicationConfig applicationConfig() throws IOException {
        return new ApplicationConfigProvider()
                .fromDefaultResource()
                .fromEnvVar("JACSSTORAGE_CONFIG")
                .fromMap(ApplicationConfigProvider.applicationArgs())
                .build();
    }
}
