package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.filter.JWTAuthFilter;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.junit.Test;

import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AgentStorageResourceTest extends AbstractCdiInjectedResourceTest {

    public static class AgentStorageDependenciesProducer {
        private DataStorageService dataStorageService = mock(DataStorageService.class);
        private StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
        private StorageLookupService storageLookupService = mock(StorageLookupService.class);
        private AgentState agentState = mock(AgentState.class);
        private JWTAuthFilter jwtAuthFilter = mock(JWTAuthFilter.class);

        @Produces
        public DataStorageService getDataStorageService() {
            return dataStorageService;
        }

        @Produces @LocalInstance
        public StorageAllocatorService getStorageAllocatorService() {
            return storageAllocatorService;
        }

        @Produces @LocalInstance
        public StorageLookupService getStorageLookupService() {
            return storageLookupService;
        }

        @Produces
        public AgentState getAgentState() {
            return agentState;
        }

        @Produces
        public ObjectMapper getObjectMapper() {
            return new ObjectMapper();
        }

        @Produces
        public JWTAuthFilter getJwtAuthFilter() {
            return jwtAuthFilter;
        }

    }

    private AgentStorageDependenciesProducer dependenciesProducer = new AgentStorageDependenciesProducer();

    public class ResourceBinder extends AbstractBinder {
        @Override
        protected void configure() {
            Annotation localInstanceAnnotation;
            try {
                Method m = AgentStorageDependenciesProducer.class.getMethod("getStorageLookupService");
                localInstanceAnnotation = m.getAnnotation(LocalInstance.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException(e);
            }
            bind(dependenciesProducer.getDataStorageService()).to(DataStorageService.class);
            bind(dependenciesProducer.getStorageAllocatorService()).qualifiedBy(localInstanceAnnotation).to(StorageAllocatorService.class);
            bind(dependenciesProducer.getStorageLookupService()).qualifiedBy(localInstanceAnnotation).to(StorageLookupService.class);
        }
    }

    @Override
    protected JAXAgentStorageApp configure() {
        return new JAXAgentStorageApp() {
            @Override
            protected Set<Class<?>> getAppClasses() {
                return ImmutableSet.<Class<?>>builder()
                        .addAll(super.getAppClasses())
                        .build()
                ;
            }

            @Override
            public Set<Object> getSingletons() {
                return ImmutableSet.builder()
                        .addAll(super.getSingletons())
                        .add(new ResourceBinder())
                        .build()
                        ;
            }
        };
    }

    @Override
    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[] {
                AgentStorageDependenciesProducer.class
        };
    }

    @Test
    public void retrieveStream() throws IOException {
        StorageLookupService storageLookupService = dependenciesProducer.getStorageLookupService();
        Long testBundleId = 1L;
        String testPath = "testPath";
        JacsStorageFormat testFormat = JacsStorageFormat.DATA_DIRECTORY;
        when(storageLookupService.getDataBundleById(testBundleId))
                .thenReturn(new JacsBundleBuilder()
                        .path(testPath)
                        .storageFormat(testFormat)
                        .build());
        DataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        String testData = "Test data";
        when(dataStorageService.retrieveDataStream(eq(Paths.get(testPath)), eq(testFormat), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(2);
                    String checksum = "check";
                    out.write(testData.getBytes());
                    return new TransferInfo(testData.length(), checksum.getBytes());
                });
        InputStream response = target().path(Constants.AGENTSTORAGE_URI_PATH).path(testBundleId.toString()).request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

}
