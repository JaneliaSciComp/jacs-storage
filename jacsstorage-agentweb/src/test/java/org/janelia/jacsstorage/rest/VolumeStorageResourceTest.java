package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.helper.OriginalStorageResourceHelper;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.OriginalDataStorageService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PathBasedAgentStorageResource.class, OriginalStorageResourceHelper.class})
public class VolumeStorageResourceTest extends AbstractCdiInjectedResourceTest {

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
    public void retrieveContent() throws IOException {
        Long testStorageVolumeId = 10L;
        String testPath = "d1/d2/f1";
        String testPhysicalRoot = "/storageRoot";
        Path testFullPath = Paths.get(testPhysicalRoot, testPath);
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        PowerMockito.mockStatic(Files.class);
        when(Files.exists(testFullPath)).thenReturn(true);
        when(Files.isRegularFile(testFullPath)).thenReturn(true);
        OriginalDataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        String testContent = "This is the content";
        when(dataStorageService.estimateDataEntrySize(eq(testFullPath), eq(""), eq(JacsStorageFormat.SINGLE_DATA_FILE), any(ContentFilterParams.class)))
                .thenReturn((long) testContent.length());
        when(dataStorageService.retrieveDataStream(eq(testFullPath), eq(JacsStorageFormat.SINGLE_DATA_FILE), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(3);
                    os.write(testContent.getBytes());
                    return (long) testContent.length();
                });
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(testStorageVolumeId.toString())
                .path("data_content")
                .path(testPath)
                .request()
                .get();
        assertEquals(200, response.getStatus());
        assertArrayEquals(testContent.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));

    }

    @Test
    public void fileNotFound() throws IOException {
        Long testStorageVolumeId = 10L;
        String testPath = "d1/d2/f1";
        String testPhysicalRoot = "/storageRoot";
        Path testFullPath = Paths.get(testPhysicalRoot, testPath);
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        PowerMockito.mockStatic(Files.class);
        when(Files.exists(testFullPath)).thenReturn(false);
        when(Files.notExists(testFullPath)).thenReturn(true);
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(testStorageVolumeId.toString())
                .path("data_content")
                .path(testPath)
                .request()
                .get();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void exceptionThrownWhileEstimatingTheSize() throws IOException {
        Long testStorageVolumeId = 10L;
        String testPath = "d1/d2/f1";
        String testPhysicalRoot = "/storageRoot";
        Path testFullPath = Paths.get(testPhysicalRoot, testPath);
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        PowerMockito.mockStatic(Files.class);
        when(Files.exists(testFullPath)).thenReturn(true);
        when(Files.isRegularFile(testFullPath)).thenReturn(true);
        OriginalDataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        UncheckedIOException thrownException = new UncheckedIOException(new IOException("error getting file size"));
        when(dataStorageService.estimateDataEntrySize(eq(testFullPath), eq(""), eq(JacsStorageFormat.SINGLE_DATA_FILE), any(ContentFilterParams.class)))
                .thenThrow(thrownException);
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(testStorageVolumeId.toString())
                .path("data_content")
                .path(testPath)
                .request()
                .get();
        assertEquals(500, response.getStatus());
        ErrorResponse errorResponse = response.readEntity(ErrorResponse.class);
        assertThat(errorResponse.getErrorMessage(), equalTo(thrownException.getMessage()));
    }

    @Test
    public void exceptionThrownWhileRetrievingContent() throws IOException {
        Long testStorageVolumeId = 10L;
        String testPath = "d1/d2/f1";
        String testPhysicalRoot = "/storageRoot";
        Path testFullPath = Paths.get(testPhysicalRoot, testPath);
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        PowerMockito.mockStatic(Files.class);
        when(Files.exists(testFullPath)).thenReturn(true);
        when(Files.isRegularFile(testFullPath)).thenReturn(true);
        OriginalDataStorageService dataStorageService = dependenciesProducer.getDataStorageService();
        when(dataStorageService.estimateDataEntrySize(eq(testFullPath), eq(""), eq(JacsStorageFormat.SINGLE_DATA_FILE), any(ContentFilterParams.class)))
                .thenReturn(10L);
        when(dataStorageService.retrieveDataStream(eq(testFullPath), eq(JacsStorageFormat.SINGLE_DATA_FILE), any(ContentFilterParams.class), any(OutputStream.class)))
                .thenThrow(new IOException("error reading file"));
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(testStorageVolumeId.toString())
                .path("data_content")
                .path(testPath)
                .request()
                .get();
        assertEquals(500, response.getStatus());
    }
}
