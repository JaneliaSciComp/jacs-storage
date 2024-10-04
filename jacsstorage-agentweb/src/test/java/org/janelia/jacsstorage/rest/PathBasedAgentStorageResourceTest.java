package org.janelia.jacsstorage.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Set;

import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.ContentGetter;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PathBasedAgentStorageResourceTest extends AbstractCdiInjectedResourceTest {

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
    public void retrieveDataStreamUsingDataPathRelativeToVolumeRoot() throws IOException {
        String testPath = "/volRoot/testPath";
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volBinding")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volRoot/testPath");
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_path/data_content")
                .path(expectedDataURI.getJadeStorage())
                .request()
                .get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamUsingDataPathRelativeToVolumeBindingPath() throws IOException {
        String testPath = "/volBinding/testPath";
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volBinding")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volRoot/testPath");
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(testPath).request().get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamUsingDataPathURIRelativeToVolumeBindingPath() throws IOException {
        String[] testPaths = new String[] {
                "jade://volBinding/testPath",
                "jade:///volBinding/testPath"
        };
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath("/volBinding/testPath"))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volBinding")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI("/volRoot/testPath");
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        for (String testPath : testPaths) {
            Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(testPath).request().get();
            assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
            assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
        }
    }

    @Test
    public void retrieveDataStreamUsingDataPathWithCharsThatShouldBeEscaped() throws IOException {
        String testPath = "/volRoot/testPath/c1/c2-5%/testFile";
        String urlEncodedPath = StringUtils.replace(testPath, "%", "%25");
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(eq(new StorageQuery().setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                                new JacsStorageVolumeBuilder()
                                        .storageVirtualPath("/volBinding")
                                        .storageRootTemplate("/volRoot")
                                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                        .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI(testPath);
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(urlEncodedPath).request().get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamFromS3UsingAWSS3URI() throws IOException {
        String testPath = "s3://testBucket/testPrefix/test.key";
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(
                eq(new StorageQuery().setStorageType(JacsStorageType.S3).setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                                new JacsStorageVolumeBuilder()
                                        .storageType(JacsStorageType.S3)
                                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                        .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI(testPath);
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_path/data_content")
                .path(expectedDataURI.getJadeStorage())
                .request()
                .get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamFromS3UsingEndpointURI() throws IOException {
        String testPath = "https://user:secret@testEndpoint/testBucket/testPrefix/test.key";
        DataContentService storageContentReader = dependenciesProducer.getDataContentService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.findVolumes(
                eq(new StorageQuery().setStorageType(JacsStorageType.S3).setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                                new JacsStorageVolumeBuilder()
                                        .storageType(JacsStorageType.S3)
                                        .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                        .build()
                        )
                );
        String testData = "Test data";
        JADEStorageURI expectedDataURI = JADEStorageURI.createStoragePathURI(testPath);
        ContentGetter testContentGetter = mock(ContentGetter.class);
        when(storageContentReader.getDataContent(eq(expectedDataURI), any(ContentAccessParams.class)))
                .thenReturn(testContentGetter);
        when(testContentGetter.estimateContentSize()).thenReturn((long) testData.length());
        when(testContentGetter.streamContent(any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream os = invocation.getArgument(0);
                    os.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_path/data_content")
                .path(expectedDataURI.getJadeStorage())
                .request()
                .get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

}
