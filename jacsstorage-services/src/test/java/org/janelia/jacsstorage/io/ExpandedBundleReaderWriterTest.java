package org.janelia.jacsstorage.io;

import org.apache.commons.compress.utils.IOUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.gt;

public class ExpandedBundleReaderWriterTest {

    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private ExpandedArchiveBundleReader expandedBundleReader;
    private ExpandedArchiveBundleWriter expandedArchiveBundleWriter;

    @Before
    public void setUp() throws IOException {
        expandedBundleReader = new ExpandedArchiveBundleReader();
        expandedArchiveBundleWriter = new ExpandedArchiveBundleWriter();
        testDirectory = Files.createTempDirectory("ExpandedBundleReaderWriterTest");
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
    public void directoryReadWriteCheck() throws Exception {
        Path testDataDir = Paths.get(TEST_DATA_DIRECTORY);
        Path testFilePath = testDirectory.resolve("readBundleToStream");
        Path testExpandedPath = testDirectory.resolve("expandedArchiveBundle");
        OutputStream testOutputStream = null;
        InputStream testInputStream = null;
        try {
            testOutputStream = new FileOutputStream(testFilePath.toFile());
            TransferInfo sentInfo = expandedBundleReader.readBundle(TEST_DATA_DIRECTORY, testOutputStream);
            assertNotNull(sentInfo);
            testOutputStream.close();
            testOutputStream = null;
            testInputStream = new BufferedInputStream(new FileInputStream(testFilePath.toFile()));
            TransferInfo receivedInfo = expandedArchiveBundleWriter.writeBundle(testInputStream, testExpandedPath.toString());
            assertNotNull(receivedInfo);
            assertEquals(sentInfo.getNumBytes(), receivedInfo.getNumBytes());
            assertArrayEquals(sentInfo.getChecksum(), receivedInfo.getChecksum());
            Files.walkFileTree(testExpandedPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Path relativePath = testExpandedPath.relativize(file);
                    assertTrue(Files.exists(testDataDir.resolve(relativePath)));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Path relativePath = testExpandedPath.relativize(dir);
                    assertTrue(Files.exists(testDataDir.resolve(relativePath)));
                    return FileVisitResult.CONTINUE;
                }
            });
        } finally {
            IOUtils.closeQuietly(testOutputStream);
            IOUtils.closeQuietly(testInputStream);
        }
    }

    @Test
    public void fileBundleRead() throws IOException {
        Path testDataPath = Paths.get(TEST_DATA_DIRECTORY, "f_1_1");
        byte[] testDataBytes = Files.readAllBytes(testDataPath);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TransferInfo info = expandedBundleReader.readBundle(testDataPath.toString(), output);
        assertThat(info.getNumBytes(), Matchers.equalTo((long) testDataBytes.length));
        assertThat(output.toByteArray().length, Matchers.greaterThan(testDataBytes.length));
    }

    @Test
    public void bundleReadFailureBecauseSourceIsMissing() {
        Path testDataPath = Paths.get(TEST_DATA_DIRECTORY, "missing");

        assertThatThrownBy(() -> expandedBundleReader.readBundle(testDataPath.toString(), new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: No path found for " + testDataPath.toString());
    }
}
