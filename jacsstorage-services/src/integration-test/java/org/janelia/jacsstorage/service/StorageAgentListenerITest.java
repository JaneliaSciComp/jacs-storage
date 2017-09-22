package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.cdi.CoreCdiProducer;
import org.janelia.jacsstorage.client.SocketStorageClient;
import org.janelia.jacsstorage.client.StorageClient;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.ExpandedArchiveBundleReader;
import org.janelia.jacsstorage.io.ExpandedArchiveBundleWriter;
import org.janelia.jacsstorage.io.SingleFileBundleReader;
import org.janelia.jacsstorage.io.SingleFileBundleWriter;
import org.janelia.jacsstorage.io.TarArchiveBundleReader;
import org.janelia.jacsstorage.io.TarArchiveBundleWriter;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;
import org.janelia.jacsstorage.protocol.StorageProtocolImpl;
import org.janelia.jacsstorage.utils.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.enterprise.inject.Instance;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageAgentListenerITest {

    private static final String TEST_DATA_DIRECTORY = "src/integration-test/resources/testdata/bundletransfer";
    private Path testDirectory;

    private DataBundleIOProvider dataBundleIOProvider;
    private StorageAgentListener socketStorageListener;
    private String listenerSocketAddr;

    @Before
    public void setUp() throws IOException {
        Instance<BundleReader> bundleReaderSource = mock(Instance.class);
        Instance<BundleWriter> bundleWriterSource = mock(Instance.class);
        List<BundleReader> dataReaders = ImmutableList.<BundleReader>of(
                new SingleFileBundleReader(),
                new TarArchiveBundleReader(),
                new ExpandedArchiveBundleReader()
        );
        List<BundleWriter> dataWriters = ImmutableList.<BundleWriter>of(
                new SingleFileBundleWriter(),
                new TarArchiveBundleWriter(),
                new ExpandedArchiveBundleWriter()
        );
        when(bundleReaderSource.iterator())
                .then(invocation -> dataReaders.iterator());
        when(bundleWriterSource.iterator())
                .then(invocation -> dataWriters.iterator());
        CoreCdiProducer cdiProducer = new CoreCdiProducer();
        dataBundleIOProvider = new DataBundleIOProvider(bundleReaderSource, bundleWriterSource);
        socketStorageListener = new StorageAgentListener("localhost", 10000, Executors.newSingleThreadExecutor(), dataBundleIOProvider);
        testDirectory = Files.createTempDirectory("StorageListenerITest");
        startListener(cdiProducer.createStorageAgentExecutor());
    }

    private void startListener(ExecutorService agentExecutor) {
        CountDownLatch started = new CountDownLatch(1);
        agentExecutor.execute(() -> {
            try {
                listenerSocketAddr = socketStorageListener.open();
                started.countDown();
                socketStorageListener.startServer();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        try {
            started.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private StorageClient createStorageClient() {
        return new SocketStorageClient(
                new StorageProtocolImpl(Executors.newSingleThreadExecutor(), dataBundleIOProvider),
                new StorageProtocolImpl(Executors.newSingleThreadExecutor(), dataBundleIOProvider)
        );
    }

    @After
    public void tearDown() throws IOException {
        PathUtils.deletePath(testDirectory);
    }

    @Test
    public void sendDataDirectory() throws IOException {
        class TestData {
            final StorageClient storageClient;
            final DataStorageInfo persistedDataStorageInfo;
            final DataStorageInfo retrievedDataStorageInfo;

            public TestData(StorageClient storageClient, DataStorageInfo persistedDataStorageInfo, DataStorageInfo retrievedDataStorageInfo) {
                this.storageClient = storageClient;
                this.persistedDataStorageInfo = persistedDataStorageInfo;
                this.retrievedDataStorageInfo = retrievedDataStorageInfo;
            }
        }
        List<TestData> testData = ImmutableList.<TestData>builder()
                .add(new TestData(
                                createStorageClient(),
                                storageInfo(testDirectory.resolve("td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td1.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                createStorageClient(),
                                storageInfo(testDirectory.resolve("td2.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("td2.local"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                createStorageClient(),
                                storageInfo(testDirectory.resolve("td3.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td3.local"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                createStorageClient(),
                                storageInfo(testDirectory.resolve("td4.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("td4.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .build();
        Path sourceTestDataDirectory = Paths.get(TEST_DATA_DIRECTORY);
        long sourceTestDataDirSize = PathUtils.getSize(sourceTestDataDirectory);
        for (TestData td : testData) {
            StorageMessageResponse persistenceResponse = td.storageClient.persistData(TEST_DATA_DIRECTORY, td.persistedDataStorageInfo);
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(persistenceResponse.getStatus(), equalTo(1));
            assertTrue(Files.exists(targetPath));
            assertThat(PathUtils.getSize(targetPath), lessThanOrEqualTo(persistenceResponse.getSize()));
            StorageMessageResponse retrievalResponse = td.storageClient.retrieveData(td.retrievedDataStorageInfo.getPath(), td.persistedDataStorageInfo);
            Path localPath = Paths.get(td.retrievedDataStorageInfo.getPath());
            assertThat(retrievalResponse.getStatus(), equalTo(1));
            assertTrue(Files.exists(localPath));
            assertThat(PathUtils.getSize(localPath), equalTo(sourceTestDataDirSize));
            assertThat(retrievalResponse.getSize(), equalTo(persistenceResponse.getSize()));
        }
    }

    private DataStorageInfo storageInfo(Path targetPath, JacsStorageFormat storageFormat) throws IOException {
        return new DataStorageInfo()
                .setConnectionInfo(listenerSocketAddr)
                .setStorageFormat(storageFormat)
                .setPath(targetPath.toString());
    }
}
