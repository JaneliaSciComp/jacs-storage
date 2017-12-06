package org.janelia.jacsstorage.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.filter.JWTAuthFilter;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.enterprise.inject.Produces;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AgentStorageResource.class})
public class AgentStorageResourceTest extends AbstractCdiInjectedResourceTest {

    public static class AgentStorageDependenciesProducer {
        private DataStorageService dataStorageService = mock(DataStorageService.class);
        private StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
        private StorageLookupService storageLookupService = mock(StorageLookupService.class);
        private StorageVolumeManager storageVolumeManager = mock(StorageVolumeManager.class);
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

        @Produces @LocalInstance
        public StorageVolumeManager getStorageVolumeManager() {
            return storageVolumeManager;
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
            bind(dependenciesProducer.getStorageVolumeManager()).qualifiedBy(localInstanceAnnotation).to(StorageVolumeManager.class);
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
        String testPath = "volPrefix/testPath";
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
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storagePathPrefix("/volPrefix")
                                .storageRootDir("/volRoot")
                                .build()
                        )
                );
        InputStream response = target().path(Constants.AGENTSTORAGE_URI_PATH).path(testBundleId.toString()).request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

    @Test
    public void retrieveDataStreamUsingDataPathRelativeToVolumeRoot() throws IOException {
        String testPath = "volPrefix/testPath";
        DataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storagePathPrefix("/volPrefix")
                                .storageRootDir("/volRoot")
                                .build()
                        )
                );
        PowerMockito.mockStatic(Files.class);
        Path expectedDataPath = Paths.get("/volRoot/testPath");
        Mockito.when(Files.exists(eq(expectedDataPath))).thenReturn(true);
        Mockito.when(Files.isRegularFile(eq(expectedDataPath))).thenReturn(true);
        String testData = "Test data";
        JacsStorageFormat testFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        when(dataStorageService.retrieveDataStream(eq(expectedDataPath), eq(testFormat), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(2);
                    String checksum = "check";
                    out.write(testData.getBytes());
                    return new TransferInfo(testData.length(), checksum.getBytes());
                });
        InputStream response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("path").path(testPath).request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

    @Test
    public void retrieveDataStreamUsingDataPathRelativeToVolumePrefix() throws IOException {
        String testPath = "volPrefix/testPath";
        DataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storagePathPrefix("/volPrefix")
                                .storageRootDir("/volRoot")
                                .build()
                        )
                );
        PowerMockito.mockStatic(Files.class);
        Path expectedDataPath = Paths.get("/volPrefix/testPath");
        Mockito.when(Files.exists(eq(Paths.get("/volRoot", "testPath")))).thenReturn(false);
        Mockito.when(Files.exists(eq(expectedDataPath))).thenReturn(true);
        Mockito.when(Files.isRegularFile(eq(expectedDataPath))).thenReturn(true);
        String testData = "Test data";
        JacsStorageFormat testFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        when(dataStorageService.retrieveDataStream(eq(expectedDataPath), eq(testFormat), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(2);
                    String checksum = "check";
                    out.write(testData.getBytes());
                    return new TransferInfo(testData.length(), checksum.getBytes());
                });
        InputStream response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("path").path(testPath).request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

}
