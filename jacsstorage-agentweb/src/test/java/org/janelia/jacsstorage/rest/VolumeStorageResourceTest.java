package org.janelia.jacsstorage.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Set;

import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

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
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        String testContent = "This is the content";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI(testPhysicalRoot).resolve(testPath);
        when(storageContentReader.readDataStream(eq(expectedDataURI), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(2);
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
    public void exceptionThrownWhileRetrievingContent() {
        Long testStorageVolumeId = 10L;
        String testPath = "d1/d2/f1";
        String testPhysicalRoot = "/storageRoot";
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getVolumeById(testStorageVolumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(testStorageVolumeId)
                        .storageVirtualPath("/virtualRoot")
                        .storageRootTemplate(testPhysicalRoot)
                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                        .build()
                );
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI(testPhysicalRoot).resolve(testPath);
        when(storageContentReader.readDataStream(eq(expectedDataURI), any(ContentFilterParams.class), any(OutputStream.class)))
                .thenThrow(new ContentException("error reading file"));
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
