package org.janelia.jacsstorage.cdi;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.TimebasedIdGenerator;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.janelia.jacsstorage.datatransfer.impl.DataTransferServiceImpl;
import org.janelia.jacsstorage.io.DataBundleIOProvider;

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
        return applicationConfig.getStringPropertyValue(property.name(), property.defaultValue());
    }

    @Produces
    @PropertyValue(name = "")
    public Boolean booleanPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        String defaultValue = property.defaultValue();
        if (StringUtils.isBlank(defaultValue)) {
            return applicationConfig.getBooleanPropertyValue(property.name());
        } else {
            return applicationConfig.getBooleanPropertyValue(property.name(), Boolean.valueOf(defaultValue));
        }
    }

    @Produces
    @PropertyValue(name = "")
    public Integer integerPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        String defaultValue = property.defaultValue();
        if (StringUtils.isBlank(defaultValue)) {
            return applicationConfig.getIntegerPropertyValue(property.name());
        } else {
            return applicationConfig.getIntegerPropertyValue(property.name(), Integer.valueOf(defaultValue));
        }
    }

    @Produces
    @PropertyValue(name = "")
    public Long longPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        String defaultValue = property.defaultValue();
        if (StringUtils.isBlank(defaultValue)) {
            return applicationConfig.getLongPropertyValue(property.name());
        } else {
            return applicationConfig.getLongPropertyValue(property.name(), Long.valueOf(defaultValue));
        }
    }

    @Produces
    @PropertyValue(name = "")
    public List<String> listPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        final PropertyValue property = injectionPoint.getAnnotated().getAnnotation(PropertyValue.class);
        String defaultValue = property.defaultValue();
        if (StringUtils.isBlank(defaultValue)) {
            return applicationConfig.getStringListPropertyValue(property.name());
        } else {
            return applicationConfig.getStringListPropertyValue(property.name(), Splitter.on(',').trimResults().splitToList(defaultValue));
        }
    }

    @Produces
    @PropertyValue(name = "")
    public Set<String> setPropertyValue(@ApplicationProperties ApplicationConfig applicationConfig, InjectionPoint injectionPoint) {
        return ImmutableSet.copyOf(listPropertyValue(applicationConfig, injectionPoint));
    }

    @ApplicationScoped
    @ApplicationProperties
    @Produces
    public ApplicationConfig applicationConfig() {
        return new ApplicationConfigProvider()
                .fromDefaultResources()
                .fromEnvVar("JACSSTORAGE_CONFIG")
                .fromMap(ApplicationConfigProvider.getAppDynamicArgs())
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

    @ScheduledResource
    @Produces
    public ScheduledExecutorService createScheduledExecutorService() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-STORAGE-TASK-%d")
                .setDaemon(true)
                .build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    public void shutdownExecutor(@Disposes @ScheduledResource ScheduledExecutorService executorService) throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @PooledResource
    @Produces
    public DataTransferService createPooledStorageProtocol(@PooledResource ExecutorService pooledExecutorService, DataBundleIOProvider dataIOProvider) {
        return new DataTransferServiceImpl(pooledExecutorService, dataIOProvider);
    }
}
