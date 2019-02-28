package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.service.StorageContentReader;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PathBasedAgentStorageResource.class, StorageResourceHelper.class, PathUtils.class})
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
        StorageContentReader storageContentReader = dependenciesProducer.getDataStorageService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        String testData = "Test data";
        PowerMockito.mockStatic(Files.class);
        PowerMockito.mockStatic(PathUtils.class);
        Path expectedDataPath = Paths.get("/volRoot/testPath");
        Mockito.when(Files.exists(eq(expectedDataPath))).thenReturn(true);
        Mockito.when(Files.isRegularFile(eq(expectedDataPath))).thenReturn(true);
        JacsStorageFormat testFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        when(storageContentReader.estimateDataEntrySize(eq(expectedDataPath), eq(""), eq(testFormat), any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());
        when(storageContentReader.retrieveDataStream(eq(expectedDataPath), eq(testFormat), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(3);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(StoragePathURI.createAbsolutePathURI(testPath).toString()).request().get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamUsingDataPathRelativeToVolumePrefix() throws IOException {
        String testPath = "/volPrefix/testPath";
        StorageContentReader storageContentReader = dependenciesProducer.getDataStorageService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath(testPath))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        PowerMockito.mockStatic(Files.class);
        PowerMockito.mockStatic(PathUtils.class);
        String testData = "Test data";
        Path expectedDataPath = Paths.get("/volRoot/testPath");
        Mockito.when(Files.exists(eq(expectedDataPath))).thenReturn(true);
        Mockito.when(Files.exists(eq(Paths.get("/volPrefix", "testPath")))).thenReturn(false);
        Mockito.when(Files.isRegularFile(eq(expectedDataPath))).thenReturn(true);
        JacsStorageFormat testFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        when(storageContentReader.estimateDataEntrySize(eq(expectedDataPath), eq(""), eq(testFormat), any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());
        when(storageContentReader.retrieveDataStream(eq(expectedDataPath), eq(testFormat), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(3);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(testPath).request().get();
        assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
        assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
    }

    @Test
    public void retrieveDataStreamUsingDataPathURIRelativeToVolumePrefix() throws IOException {
        String[] testPaths = new String[] {
                "jade://volPrefix/testPath",
                "jade:///volPrefix/testPath"
        };
        StorageContentReader storageContentReader = dependenciesProducer.getDataStorageService();
        StorageVolumeManager storageVolumeManager = dependenciesProducer.getStorageVolumeManager();
        when(storageVolumeManager.getManagedVolumes(eq(new StorageQuery().setDataStoragePath("/volPrefix/testPath"))))
                .thenReturn(ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageVirtualPath("/volPrefix")
                                .storageRootTemplate("/volRoot")
                                .volumePermissions(EnumSet.of(JacsStoragePermission.READ))
                                .build()
                        )
                );
        PowerMockito.mockStatic(Files.class);
        PowerMockito.mockStatic(PathUtils.class);
        String testData = "Test data";
        Path expectedDataPath = Paths.get("/volRoot/testPath");
        Mockito.when(Files.exists(eq(expectedDataPath))).thenReturn(true);
        Mockito.when(Files.exists(eq(Paths.get("/volPrefix", "testPath")))).thenReturn(false);
        Mockito.when(Files.isRegularFile(eq(expectedDataPath))).thenReturn(true);
        JacsStorageFormat testFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        when(storageContentReader.estimateDataEntrySize(eq(expectedDataPath), eq(""), eq(testFormat), any(ContentFilterParams.class)))
                .then(invocation -> (long) testData.length());
        when(storageContentReader.retrieveDataStream(eq(expectedDataPath), eq(testFormat), any(ContentFilterParams.class), any(OutputStream.class)))
                .then(invocation -> {
                    OutputStream out = invocation.getArgument(3);
                    out.write(testData.getBytes());
                    return (long) testData.length();
                });
        for (String testPath : testPaths) {
            Response response = target().path(Constants.AGENTSTORAGE_URI_PATH).path("storage_path/data_content").path(testPath).request().get();
            assertEquals(String.valueOf(testData.length()), response.getHeaderString("Content-Length"));
            assertArrayEquals(testData.getBytes(), ByteStreams.toByteArray(response.readEntity(InputStream.class)));
        }
    }

}
