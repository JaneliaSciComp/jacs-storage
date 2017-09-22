package org.janelia.jacsstorage.protocol;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.SingleFileBundleReader;
import org.janelia.jacsstorage.io.SingleFileBundleWriter;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.enterprise.inject.Instance;
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

import static org.hamcrest.collection.IsIn.isIn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageProtocolImplTest {

    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private StorageProtocolImpl storageAgentImpl;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws IOException {
        Instance<BundleReader> bundleReaderSource = mock(Instance.class);
        Instance<BundleWriter> bundleWriterSource = mock(Instance.class);
        when(bundleReaderSource.iterator()).thenReturn(ImmutableList.<BundleReader>of(new SingleFileBundleReader()).iterator());
        when(bundleWriterSource.iterator()).thenReturn(ImmutableList.<BundleWriter>of(new SingleFileBundleWriter()).iterator());
        storageAgentImpl = new StorageProtocolImpl(Executors.newSingleThreadExecutor(), new DataBundleIOProvider(bundleReaderSource, bundleWriterSource));
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
        byte[] headerBuffer = storageAgentImpl.encodeMessageHeader(new StorageMessageHeader(
                StorageProtocol.Operation.PERSIST_DATA,
                JacsStorageFormat.SINGLE_DATA_FILE,
                testTargetPath.toString()));
        FileInputStream testInput = new FileInputStream(testDataPath.toFile());
        try {
            StorageProtocol.Holder<StorageMessageHeader> requestHolder = new StorageProtocol.Holder<>();
            while (!storageAgentImpl.readMessageHeader(ByteBuffer.wrap(headerBuffer), requestHolder))
                ; // keep reading until it finishes the header
            storageAgentImpl.beginDataTransfer(requestHolder.getData());

            ByteBuffer buffer = ByteBuffer.allocate(2048);
            FileChannel channel = testInput.getChannel();
            while (channel.read(buffer) != -1) {
                buffer.flip();
                storageAgentImpl.writeData(buffer);
                buffer.clear();
            }

            try {
                storageAgentImpl.endDataTransfer();
                assertThat(storageAgentImpl.getState(), isIn(EnumSet.of(StorageProtocol.State.WRITE_DATA, StorageProtocol.State.WRITE_DATA_COMPLETE, StorageProtocol.State.WRITE_DATA_ERROR)));
                while (storageAgentImpl.getState() != StorageProtocol.State.WRITE_DATA_COMPLETE && storageAgentImpl.getState() != StorageProtocol.State.WRITE_DATA_ERROR) {
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
        byte[] headerBuffer = storageAgentImpl.encodeMessageHeader(new StorageMessageHeader(
                StorageProtocol.Operation.RETRIEVE_DATA,
                JacsStorageFormat.SINGLE_DATA_FILE,
                testDataPath.toString()));
        StorageProtocol.Holder<StorageMessageHeader> requestHolder = new StorageProtocol.Holder<>();
        while (!storageAgentImpl.readMessageHeader(ByteBuffer.wrap(headerBuffer), requestHolder))
            ; // keep reading until it finishes the header

        storageAgentImpl.beginDataTransfer(requestHolder.getData());

        ByteBuffer buffer = ByteBuffer.allocate(2048);
        byte[] expectedResult = Files.readAllBytes(testDataPath);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        while(storageAgentImpl.readData(buffer) != -1) {
            buffer.flip();
            result.write(buffer.array(), buffer.position(), buffer.limit());
            buffer.clear();
        }
        assertArrayEquals(expectedResult, result.toByteArray());
        assertThat(storageAgentImpl.getState(), isIn(EnumSet.of(StorageProtocol.State.READ_DATA, StorageProtocol.State.READ_DATA_COMPLETE, StorageProtocol.State.READ_DATA_ERROR)));
    }
}
