package org.janelia.jacsstorage.cdi;

import com.fasterxml.jackson.databind. ObjectMapper;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.TimebasedIdGenerator;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.protocol.StorageService;
import org.janelia.jacsstorage.protocol.StorageServiceImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    @PooledResource
    @Produces
    public ExecutorService createPooledExecutorService(@PropertyValue(name = "StorageAgent.ThreadPoolSize") Integer poolSize) {
        return Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public void shutdownExecutor(@Disposes @PooledResource ExecutorService executorService) throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @PooledResource
    @Produces
    public StorageService createPooledStorageProtocol(@PooledResource ExecutorService pooledExecutorService, DataBundleIOProvider dataIOProvider) {
        return new StorageServiceImpl(pooledExecutorService, dataIOProvider);
    }
}
