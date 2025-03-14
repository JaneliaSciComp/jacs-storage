package org.janelia.jacsstorage.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataBundleStorageResourceTest extends AbstractCdiInjectedResourceTest {

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
    public void putEntryContent() {
        Long testBundleId = 1L;

        StorageLookupService storageLookupService = dependenciesProducer.getStorageLookupService();
        String testPath = "/volPrefix/testPath";
        JacsStorageFormat testFormat = JacsStorageFormat.DATA_DIRECTORY;

        JacsBundle testBundle = new JacsBundleBuilder()
                .dataBundleId(testBundleId)
                .storageVirtualPath(testPath)
                .path(testPath)
                .storageFormat(testFormat)
                .build();

        when(storageLookupService.getDataBundleById(testBundleId))
                .thenReturn(testBundle);

        String testDataEntryName = "d_1/d_1_1/e_1_1_1";
        String testDataContent = "Test data";
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .path("data_content")
                .path(testDataEntryName)
                .request(MediaType.APPLICATION_JSON)
                .put(Entity.entity(new ByteArrayInputStream(testDataContent.getBytes()), MediaType.APPLICATION_OCTET_STREAM));
        assertEquals(201, response.getStatus());

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
        DataContentService dataContentService = dependenciesProducer.getDataContentService();
        String testDataContent = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volPrefix/testPath/e1", JADEOptions.create());
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(dataContentService.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testDataContent.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testDataContent.getBytes());
                    return (long) testDataContent.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
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
                .path("data_content")
                .path(testDataEntryName)
                .queryParam("selectedEntries", "v1", "v2")
                .request(MediaType.APPLICATION_OCTET_STREAM).get(InputStream.class);
        assertArrayEquals(testDataContent.getBytes(), ByteStreams.toByteArray(response));
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
        DataContentService dataContentService = dependenciesProducer.getDataContentService();
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volPrefix/testPath", JADEOptions.create());
        String testDataContent = "Test data";
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(dataContentService.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testDataContent.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testDataContent.getBytes());
                    return (long) testDataContent.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
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
                .path("data_content")
                .queryParam("selectedEntries", "v1", "v2")
                .request(MediaType.APPLICATION_OCTET_STREAM).get(InputStream.class);
        assertArrayEquals(testDataContent.getBytes(), ByteStreams.toByteArray(response));
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
        DataContentService dataContentService = dependenciesProducer.getDataContentService();
        String testDataContent = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volPrefix/testPath", JADEOptions.create());
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(dataContentService.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testDataContent.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testDataContent.getBytes());
                    return (long) testDataContent.length();
                });
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath("/" + testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot/${username}")
                                .build()
                        )
                );
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(testBundleId.toString())
                .path("data_contente1")
                .queryParam("selectedEntries", "v1", "v2")
                .request().get();
        assertNotEquals(200, response.getStatus());
    }
}
