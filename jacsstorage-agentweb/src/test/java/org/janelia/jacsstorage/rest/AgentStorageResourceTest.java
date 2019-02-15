package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class AgentStorageResourceTest extends AbstractCdiInjectedResourceTest {

    private TestAgentStorageDependenciesProducer dependenciesProducer = new TestAgentStorageDependenciesProducer();

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
                        .add(new TestResourceBinder(dependenciesProducer))
                        .build()
                        ;
            }
        };
    }

    @Override
    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[] {
                TestAgentStorageDependenciesProducer.class
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
        when(dataStorageService.retrieveDataStream(eq(Paths.get(testPath)), eq(testFormat), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(3);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot/${owner}")
                                .build()
                        )
                );
        InputStream response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

    @Test
    public void retrieveEntryContent() throws IOException {
        StorageLookupService storageLookupService = dependenciesProducer.getStorageLookupService();
        Long testBundleId = 1L;
        String testPath = "volPrefix/testPath";
        String testDataEntryName = "e1";
        JacsStorageFormat testFormat = JacsStorageFormat.DATA_DIRECTORY;
        when(storageLookupService.getDataBundleById(testBundleId))
                .thenReturn(new JacsBundleBuilder()
                        .path(testPath)
                        .storageFormat(testFormat)
                        .build());
        DataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        String testData = "Test data";
        when(dataStorageService.estimateDataEntrySize(
                eq(Paths.get(testPath)),
                eq(testDataEntryName),
                eq(testFormat),
                any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());
        when(dataStorageService.readDataEntryStream(
                eq(Paths.get(testPath)),
                eq(testDataEntryName),
                eq(testFormat),
                any(ContentFilterParams.class),
                any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(4);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot/${owner}")
                                .build()
                        )
                );
        InputStream response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .path("entry_content")
                .path(testDataEntryName)
                .queryParam("selectedEntries", "v1", "v2")
                .request(MediaType.APPLICATION_OCTET_STREAM).get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

    @Test
    public void retrieveEntryContentFromDataEntryRoot() throws IOException {
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
        when(dataStorageService.estimateDataEntrySize(
                eq(Paths.get(testPath)),
                eq(""),
                eq(testFormat),
                any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());
        when(dataStorageService.readDataEntryStream(
                eq(Paths.get(testPath)),
                eq(""),
                eq(testFormat),
                any(ContentFilterParams.class),
                any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(4);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot/${owner}")
                                .build()
                        )
                );
        InputStream response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .path("entry_content")
                .queryParam("selectedEntries", "v1", "v2")
                .request().get(InputStream.class);
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response));
    }

    @Test
    public void invalidRetrieveEntryContent() throws IOException {
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
        when(dataStorageService.estimateDataEntrySize(
                eq(Paths.get(testPath)),
                eq(""),
                eq(testFormat),
                any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());

        when(dataStorageService.readDataEntryStream(
                eq(Paths.get(testPath)),
                eq(""),
                eq(testFormat),
                any(ContentFilterParams.class),
                any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(4);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot/${owner}")
                                .build()
                        )
                );
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .path("entry_contente1")
                .queryParam("selectedEntries", "v1", "v2")
                .request().get();
        assertNotEquals(200, response.getStatus());
    }
}
