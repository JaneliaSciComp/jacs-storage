package org.janelia.jacsstorage.datatransfer.impl;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.concurrent.Executors;

import javax.enterprise.inject.Instance;

import com.google.common.collect.ImmutableList;

import org.hamcrest.collection.IsIn;
import org.hamcrest.core.Is;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.janelia.jacsstorage.datatransfer.State;
import org.janelia.jacsstorage.datatransfer.StorageMessageHeader;
import org.janelia.jacsstorage.datatransfer.StorageMessageHeaderCodec;
import org.janelia.jacsstorage.datatransfer.TransferState;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.OriginalContentHandlerProvider;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.SingleFileBundleReader;
import org.janelia.jacsstorage.io.SingleFileBundleWriter;
import org.janelia.jacsstorage.io.contenthandlers.NoOPContentConverter;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataTransferServiceImplTest {

    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private DataTransferServiceImpl storageService;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws IOException {
        Instance<BundleReader> bundleReaderSource = mock(Instance.class);
        Instance<BundleWriter> bundleWriterSource = mock(Instance.class);
        OriginalContentHandlerProvider contentHandlerProvider = mock(OriginalContentHandlerProvider.class);
        Mockito.when(contentHandlerProvider.getContentConverter(any(ContentFilterParams.class)))
                .thenReturn(new NoOPContentConverter(false));
        when(bundleReaderSource.iterator()).thenReturn(ImmutableList.<BundleReader>of(new SingleFileBundleReader(contentHandlerProvider)).iterator());
        when(bundleWriterSource.iterator()).thenReturn(ImmutableList.<BundleWriter>of(new SingleFileBundleWriter()).iterator());
        storageService = new DataTransferServiceImpl(Executors.newSingleThreadExecutor(), new DataBundleIOProvider(bundleReaderSource, bundleWriterSource));
        testDirectory = Files.createTempDirectory("StorageAgentTest");
    }

    @After
    public void tearDown() throws IOException {
        Files.walkFileTree(testDirectory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void writeData() throws IOException {
        Path testDataPath = Paths.get(TEST_DATA_DIRECTORY, "f_1_1");
        Path testTargetPath = testDirectory.resolve("testWriteData");
        FileInputStream testInput = new FileInputStream(testDataPath.toFile());
        try {
            TransferState<StorageMessageHeader> transferState = createMessageHeaderTransferState(DataTransferService.Operation.PERSIST_DATA, JacsStorageFormat.SINGLE_DATA_FILE, testTargetPath.toString());
            storageService.beginDataTransfer(new ContentFilterParams(), transferState);

            ByteBuffer buffer = ByteBuffer.allocate(2048);
            FileChannel channel = testInput.getChannel();
            while (channel.read(buffer) != -1) {
                buffer.flip();
                storageService.writeData(buffer, transferState);
                buffer.clear();
            }

            try {
                storageService.endDataTransfer(transferState);

                assertThat(transferState.getState(), Is.is(IsIn.in(EnumSet.of(State.WRITE_DATA, State.WRITE_DATA_COMPLETE, State.WRITE_DATA_ERROR))));

                while (transferState.getState() != State.WRITE_DATA_COMPLETE && transferState.getState() != State.WRITE_DATA_ERROR) {
                    Thread.sleep(1); // wait until the data transfer is completed
                }
            } catch (InterruptedException e) {
            }
        } finally {
            testInput.close();
        }
        assertTrue(Files.exists(testTargetPath));
        assertEquals(Files.size(testDataPath), Files.size(testTargetPath));
    }

    @Test
    public void readData() throws IOException {
        Path testDataPath = Paths.get(TEST_DATA_DIRECTORY, "f_1_1");
        TransferState<StorageMessageHeader> transferState = createMessageHeaderTransferState(DataTransferService.Operation.RETRIEVE_DATA, JacsStorageFormat.SINGLE_DATA_FILE, testDataPath.toString());
        storageService.beginDataTransfer(new ContentFilterParams(), transferState);

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        byte[] expectedResult = Files.readAllBytes(testDataPath);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        while(storageService.readData(buffer, transferState) != -1) {
            buffer.flip();
            result.write(buffer.array(), buffer.position(), buffer.limit());
            buffer.clear();
        }
        assertArrayEquals(expectedResult, result.toByteArray());
        assertThat(transferState.getState(), is(in(EnumSet.of(State.READ_DATA, State.READ_DATA_COMPLETE, State.READ_DATA_ERROR))));
    }

    private TransferState<StorageMessageHeader> createMessageHeaderTransferState(DataTransferService.Operation op, JacsStorageFormat format, String dataFile) throws IOException {
        StorageMessageHeader messageHeader = new StorageMessageHeader(
                0L,
                "",
                op,
                format,
                dataFile,
                "");
        TransferState<StorageMessageHeader> transferState = new TransferState<>();
        StorageMessageHeaderCodec messageHeaderCodec = new StorageMessageHeaderCodec();
        assertTrue(transferState.readMessageType(
                ByteBuffer.wrap(transferState.writeMessageType(messageHeader, messageHeaderCodec)),
                messageHeaderCodec));
        return transferState;
    }
}
