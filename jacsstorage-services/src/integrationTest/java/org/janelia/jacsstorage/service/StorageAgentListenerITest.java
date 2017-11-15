package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageAgentListenerITest {

    private static final String TEST_LISTNER_HOSTNAME = "localhost";
    private static final String TEST_DATA_DIRECTORY = "src/integrationTest/resources/testdata/bundletransfer";
    private static final String TEST_AUTH_KEY = "This key must be at least 32 chars long";
    private Path testDirectory;

    private static StorageAgentListener socketStorageListener;
    private static DataBundleIOProvider dataBundleIOProvider;
    private static StorageEventLogger storageEventLogger;
    private static int listenerPortNumber;

    @BeforeClass
    public static void startListener() {
        Instance<BundleReader> bundleReaderSource = mock(Instance.class);
        Instance<BundleWriter> bundleWriterSource = mock(Instance.class);
        StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
        storageEventLogger = mock(StorageEventLogger.class);
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
        socketStorageListener = new StorageAgentListener(
                new DataTransferServiceImpl(Executors.newFixedThreadPool(3), dataBundleIOProvider),
                storageAllocatorService,
                TEST_AUTH_KEY,
                storageEventLogger);
        startListener(cdiProducer.createSingleExecutorService());
    }

    private static void startListener(ExecutorService agentExecutor) {
        CountDownLatch started = new CountDownLatch(1);
        agentExecutor.execute(() -> {
            try {
                listenerPortNumber = socketStorageListener.open(TEST_LISTNER_HOSTNAME, 0);
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
                new DataTransferServiceImpl(Executors.newSingleThreadExecutor(), dataBundleIOProvider)
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
                                testDirectory.resolve("sendDataDirectory.td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataDirectory.td1.localservice"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataDirectory.td2.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("sendDataDirectory.td2.localservice"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataDirectory.td3.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataDirectory.td3.localservice"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataDirectory.td4.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("sendDataDirectory.td4.localservice"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .build();
        Path sourceTestDataDirectory = Paths.get(TEST_DATA_DIRECTORY);
        long sourceTestDataDirSize = PathUtils.getSize(sourceTestDataDirectory, (f, fa) -> fa.isRegularFile());
        for (TestData td : testData) {
            StorageMessageResponse persistenceResponse = storageClient.persistData(sourceTestDataDirectory.toString(), td.persistedDataStorageInfo, createAuthToken());
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(persistenceResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(targetPath));
            assertThat(PathUtils.getSize(targetPath, (f, fa) -> fa.isRegularFile()), greaterThanOrEqualTo(sourceTestDataDirSize));
            StorageMessageResponse retrievalResponse = storageClient.retrieveData(td.retrievedDataStorageInfo.getPath(), td.persistedDataStorageInfo, createAuthToken());
            Path localPath = Paths.get(td.retrievedDataStorageInfo.getPath());
            assertThat(retrievalResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(localPath));
            assertThat(localPath + " size compared to " + sourceTestDataDirectory, PathUtils.getSize(localPath, (f, fa) -> fa.isRegularFile()), equalTo(sourceTestDataDirSize));
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
                                storageInfo(testDirectory.resolve("sendDataFile.td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataFile.td1.localservice"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataFile.td2.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataFile.td2.localservice"), JacsStorageFormat.SINGLE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataFile.td3.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataFile.td3.localservice"), JacsStorageFormat.DATA_DIRECTORY))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataFile.td4.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                storageInfo(testDirectory.resolve("sendDataFile.td4.localservice"), JacsStorageFormat.ARCHIVE_DATA_FILE))
                )
                .add(new TestData(
                                storageInfo(testDirectory.resolve("sendDataFile.td5.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                storageInfo(testDirectory.resolve("sendDataFile.td5.localservice"), JacsStorageFormat.SINGLE_DATA_FILE))
                )
                .build();
        Path sourceTestDataFile = Paths.get(TEST_DATA_DIRECTORY, "f_1_1");
        long sourceTestDataDirSize = PathUtils.getSize(sourceTestDataFile, (f, fa) -> fa.isRegularFile());
        for (TestData td : testData) {
            StorageMessageResponse persistenceResponse = storageClient.persistData(sourceTestDataFile.toString(), td.persistedDataStorageInfo, createAuthToken());
            Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
            assertThat(targetPath.toString(), persistenceResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(targetPath));
            assertThat(targetPath.toString(), PathUtils.getSize(targetPath, (f, fa) -> fa.isRegularFile()), greaterThanOrEqualTo(sourceTestDataDirSize));
            StorageMessageResponse retrievalResponse = storageClient.retrieveData(td.retrievedDataStorageInfo.getPath(), td.persistedDataStorageInfo, createAuthToken());
            Path localPath = Paths.get(td.retrievedDataStorageInfo.getPath());
            assertThat(retrievalResponse.getStatus(), equalTo(StorageMessageResponse.OK));
            assertTrue(Files.exists(localPath));
            assertThat(localPath.toString(), PathUtils.getSize(localPath, (p, a) -> a.isRegularFile()), equalTo(sourceTestDataDirSize));
        }
    }

    @Test
    public void sendError() throws IOException {
        class TestData {
            final String testName;
            final Path sourceDataPath;
            final DataStorageInfo persistedDataStorageInfo;
            final String expectedErrorMessage;

            public TestData(String testName,
                            Path sourceDataPath,
                            DataStorageInfo persistedDataStorageInfo,
                            String expectedErrorMessage) {
                this.testName = testName;
                this.sourceDataPath = sourceDataPath;
                this.persistedDataStorageInfo = persistedDataStorageInfo;
                this.expectedErrorMessage = expectedErrorMessage;
            }
        }
        Path sourceTestData = Paths.get(TEST_DATA_DIRECTORY);
        StorageClient storageClient = createStorageClient();
        List<TestData> testData = ImmutableList.<TestData>builder()
                .add(new TestData(
                        "Test archive already exists",
                                sourceTestData,
                                storageInfo(testDirectory.resolve("sendError.td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE),
                                "Target path %s already exists")
                )
                .add(new TestData(
                        "Test file write error",
                                sourceTestData,
                                storageInfo(testDirectory.resolve("sendError.td2.remote"), JacsStorageFormat.DATA_DIRECTORY),
                                "Error writing data: java.nio.file.FileAlreadyExistsException: %s")
                )
                .add(new TestData(
                        "Test file already exists",
                                sourceTestData.resolve("f_1_1"),
                                storageInfo(testDirectory.resolve("sendError.td3.remote"), JacsStorageFormat.SINGLE_DATA_FILE),
                                "Target path %s already exists")
                )
                .build();
        for (TestData td : testData) {
            try {
                StorageMessageResponse persistenceOKResponse = storageClient.persistData(td.sourceDataPath.toString(), td.persistedDataStorageInfo, createAuthToken());
                Path targetPath = Paths.get(td.persistedDataStorageInfo.getPath());
                assertThat(td.persistedDataStorageInfo.getPath(), persistenceOKResponse.getStatus(), equalTo(StorageMessageResponse.OK));
                assertTrue(Files.exists(targetPath));
                StorageMessageResponse persistenceErrorResponse = storageClient.persistData(td.sourceDataPath.toString(), td.persistedDataStorageInfo, createAuthToken());
                assertThat(persistenceErrorResponse.getStatus(), equalTo(StorageMessageResponse.ERROR));
                assertThat(persistenceErrorResponse.getMessage(), containsString(String.format(td.expectedErrorMessage, td.persistedDataStorageInfo.getPath())));
            } catch (Exception e) {
                e.printStackTrace();
                fail("Failure detected while running " + td.testName + " " + e);
            }
        }
    }

    @Test
    public void retrieveError() throws IOException {
        DataStorageInfo remoteStorageInfo = storageInfo(testDirectory.resolve("retrieveError.td1.remote"), JacsStorageFormat.ARCHIVE_DATA_FILE);
        DataStorageInfo localStorageInfo = storageInfo(testDirectory.resolve("retrieveError.td1.localservice"), JacsStorageFormat.ARCHIVE_DATA_FILE);
        StorageClient storageClient = createStorageClient();
        StorageMessageResponse retrieveErrorResponse = storageClient.retrieveData(localStorageInfo.getPath(), remoteStorageInfo, createAuthToken());
        assertThat(retrieveErrorResponse.getStatus(), equalTo(StorageMessageResponse.ERROR));
        assertThat(retrieveErrorResponse.getMessage(), containsString(String.format("No file found for %s", remoteStorageInfo.getPath())));
    }

    @Test
    public void ping() throws IOException {
        StorageClient storageClient = createStorageClient();
        StorageMessageResponse pingResponse = storageClient.ping(TEST_LISTNER_HOSTNAME + ":" + listenerPortNumber);
        assertThat(pingResponse.getStatus(), equalTo(StorageMessageResponse.OK));
    }

    private DataStorageInfo storageInfo(Path targetPath, JacsStorageFormat storageFormat) throws IOException {
        return new DataStorageInfo()
                .setStorageHost(TEST_LISTNER_HOSTNAME)
                .setTcpPortNo(listenerPortNumber)
                .setStorageFormat(storageFormat)
                .setPath(targetPath.toString());
    }

    private String createAuthToken() {
        try {
            JWSSigner signer = new MACSigner(TEST_AUTH_KEY.getBytes());
            JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                    .subject("test")
                    .issuer("https://test")
                    .expirationTime(new Date(System.currentTimeMillis() + 60 * 1000))
                    .claim("name", "First Last")
                    .claim("email", "test@test.com")
                    .build();
            SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claimsSet);
            signedJWT.sign(signer);
            return signedJWT.serialize();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
