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
import org.janelia.jacsstorage.protocol.StorageServiceImpl;
import org.janelia.jacsstorage.utils.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.enterprise.inject.Instance;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageAgentListenerITest {

    private static final String TEST_DATA_DIRECTORY = "src/integration-test/resources/testdata/bundletransfer";
    private Path testDirectory;

    private static StorageAgentListener socketStorageListener;
    private static DataBundleIOProvider dataBundleIOProvider;
    private static String listenerSocketAddr;

    @BeforeClass
    public static void startListener() {
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
        dataBundleIOProvider = new DataBundleIOProvider(bundleReaderSource, bundleWriterSource);
        CoreCdiProducer cdiProducer = new CoreCdiProducer();
        socketStorageListener = new StorageAgentListener("localhost", 0, new StorageServiceImpl(Executors.newFixedThreadPool(3), dataBundleIOProvider));
        startListener(cdiProducer.createSingleExecutorService());
    }

    private static void startListener(ExecutorService agentExecutor) {
        CountDownLatch started = new CountDownLatch(1);
        agentExecutor.execute(() -> {
            try {
                listenerSocketAddr = socketStorageListener.open();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                started.countDown();
            }
            try {
                socketStorageListener.startServer();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        try {
            started.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Before
    public void setUp() throws IOException {
        testDirectory = Files.createTempDirectory("StorageListenerITest");
    }

    private StorageClient createStorageClient() {
        return new SocketStorageClient(
                new StorageServiceImpl(Executors.newSingleThreadExecutor(), dataBundleIOProvider)
        );
    }

    @After
    public void tearDown() throws IOException {
        PathUtils.deletePath(testDirectory);
    }

    @Test
    public void sendDataDirectory() throws IOException {
        class TestData {
            final DataStorageInfo persistedDataStorageInfo;
            final DataStorageInfo retrievedDataStorageInfo;

            public TestData(DataStorageInfo persistedDataStorageInfo, DataStorageInfo retrievedDataStorageInfo) {
                this.persistedDataStorageInfo = persistedDataStorageInfo;
                this.retrievedDataStorageInfo = retrievedDataStorageInfo;
            }
        }
        StorageClient storageClient = createStorageClient();
        List<TestData> testData = ImmutableList.<TestData>builder()
                .add(new TestData(storageInfo(
                                testDirectory.resolve("td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td1.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td2.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("td2.local"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td3.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td3.local"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td4.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("td4.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .build();
        Path sourceTestDataDirectory = Paths.get(TEST_DATA_DIRECTORY);
        long sourceTestDataDirSize = PathUtils.getSize(sourceTestDataDirectory);
        for (TestData td : testData) {
            StorageMessageResponse persistenceResponse = storageClient.persistData(sourceTestDataDirectory.toString(), td.persistedDataStorageInfo);
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(persistenceResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(targetPath));
            assertThat(PathUtils.getSize(targetPath), lessThanOrEqualTo(persistenceResponse.getPersistedBytes()));
            StorageMessageResponse retrievalResponse = storageClient.retrieveData(td.retrievedDataStorageInfo.getPath(), td.persistedDataStorageInfo);
            Path localPath = Paths.get(td.retrievedDataStorageInfo.getPath());
            assertThat(retrievalResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(localPath));
            assertThat(PathUtils.getSize(localPath), equalTo(sourceTestDataDirSize));
            assertThat(retrievalResponse.getTransferredBytes(), equalTo(persistenceResponse.getTransferredBytes()));
        }
    }

    @Test
    public void sendDataFile() throws IOException {
        class TestData {
            final DataStorageInfo persistedDataStorageInfo;
            final DataStorageInfo retrievedDataStorageInfo;

            public TestData(DataStorageInfo persistedDataStorageInfo, DataStorageInfo retrievedDataStorageInfo) {
                this.persistedDataStorageInfo = persistedDataStorageInfo;
                this.retrievedDataStorageInfo = retrievedDataStorageInfo;
            }
        }
        StorageClient storageClient = createStorageClient();
        List<TestData> testData = ImmutableList.<TestData>builder()
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td1.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td2.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td2.local"), JacsStorageFormat.SINGLE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td3.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td3.local"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td4.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("td4.local"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("td5.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("td5.local"), JacsStorageFormat.SINGLE_DATA_FILE))
                )
                .build();
        Path sourceTestDataFile = Paths.get(TEST_DATA_DIRECTORY, "f_1_1");
        long sourceTestDataDirSize = PathUtils.getSize(sourceTestDataFile);
        for (TestData td : testData) {
            StorageMessageResponse persistenceResponse = storageClient.persistData(sourceTestDataFile.toString(), td.persistedDataStorageInfo);
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(targetPath.toString(), persistenceResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(targetPath));
            assertThat(targetPath.toString(), PathUtils.getSize(targetPath), lessThanOrEqualTo(persistenceResponse.getPersistedBytes()));
            StorageMessageResponse retrievalResponse = storageClient.retrieveData(td.retrievedDataStorageInfo.getPath(), td.persistedDataStorageInfo);
            Path localPath = Paths.get(td.retrievedDataStorageInfo.getPath());
            assertThat(retrievalResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(localPath));
            assertThat(localPath.toString(), PathUtils.getSize(localPath, (p, a) -> a.isRegularFile()), equalTo(sourceTestDataDirSize));
            assertThat(localPath.toString(), retrievalResponse.getTransferredBytes(), equalTo(persistenceResponse.getTransferredBytes()));
        }
    }

    @Test
    public void sendError() throws IOException {
        class TestData {
            final Path sourceDataPath;
            final DataStorageInfo persistedDataStorageInfo;
            final String expectedErrorMessage;

            public TestData(Path sourceDataPath,
                            DataStorageInfo persistedDataStorageInfo,
                            String expectedErrorMessage) {
                this.sourceDataPath = sourceDataPath;
                this.persistedDataStorageInfo = persistedDataStorageInfo;
                this.expectedErrorMessage = expectedErrorMessage;
            }
        }
        Path sourceTestData = Paths.get(TEST_DATA_DIRECTORY);
        StorageClient storageClient = createStorageClient();
        List<TestData> testData = ImmutableList.<TestData>builder()
                .add(new TestData(
                                sourceTestData,
                                storageInfo(testDirectory.resolve("td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                "Target path %s already exists")
                )
                .add(new TestData(
                                sourceTestData,
                                storageInfo(testDirectory.resolve("td2.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                "Error writing data: java.nio.file.FileAlreadyExistsException: %s")
                )
                .add(new TestData(
                                sourceTestData.resolve("f_1_1"),
                                storageInfo(testDirectory.resolve("td3.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                "Target path %s already exists")
                )
                .build();
        for (TestData td : testData) {
            StorageMessageResponse persistenceOKResponse = storageClient.persistData(td.sourceDataPath.toString(), td.persistedDataStorageInfo);
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(td.persistedDataStorageInfo.getPath(), persistenceOKResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(targetPath));
            StorageMessageResponse persistenceErrorResponse = storageClient.persistData(td.sourceDataPath.toString(), td.persistedDataStorageInfo);
            assertThat(persistenceErrorResponse.getStatus(), equalTo(StorageMessageResponse.ERROR));
            assertThat(persistenceErrorResponse.getMessage(), containsString(String.format(td.expectedErrorMessage, td.persistedDataStorageInfo.getPath())));
        }
    }

    @Test
    public void retrieveError() throws IOException {
        DataStorageInfo remoteStorageInfo = storageInfo(testDirectory.resolve("td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE);
        DataStorageInfo localStorageInfo = storageInfo(testDirectory.resolve("td1.local"), JacsStorageFormat.ARCHIVE_DATA_FILE);
        StorageClient storageClient = createStorageClient();
        StorageMessageResponse retrieveErrorResponse = storageClient.retrieveData(localStorageInfo.getPath(), remoteStorageInfo);
        assertThat(retrieveErrorResponse.getStatus(), equalTo(StorageMessageResponse.ERROR));
        assertThat(retrieveErrorResponse.getMessage(), containsString(String.format("No file found for %s", remoteStorageInfo.getPath())));
    }

    @Test
    public void ping() throws IOException {
        StorageClient storageClient = createStorageClient();
        StorageMessageResponse pingResponse = storageClient.ping(listenerSocketAddr);
        assertThat(pingResponse.getStatus(), equalTo(StorageMessageResponse.OK));
    }

    private DataStorageInfo storageInfo(Path targetPath, JacsStorageFormat storageFormat) throws IOException {
        return new DataStorageInfo()
                .setConnectionInfo(listenerSocketAddr)
                .setStorageFormat(storageFormat)
                .setPath(targetPath.toString());
    }
}
