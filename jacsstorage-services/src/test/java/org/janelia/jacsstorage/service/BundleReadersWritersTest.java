package org.janelia.jacsstorage.service;

import org.apache.commons.compress.utils.IOUtils;
import org.janelia.jacsstorage.service.ExpandedArchiveBundleReader;
import org.janelia.jacsstorage.service.ExpandedArchiveBundleWriter;
import org.janelia.jacsstorage.service.TarArchiveBundleReader;
import org.janelia.jacsstorage.service.TarArchiveBundleWriter;
import org.janelia.jacsstorage.service.TransferInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BundleReadersWritersTest {

    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private ExpandedArchiveBundleReader expandedBundleReader;
    private ExpandedArchiveBundleWriter expandedArchiveBundleWriter;
    private TarArchiveBundleReader tarBundleReader;
    private TarArchiveBundleWriter tarBundleWriter;

    @Before
    public void setUp() throws IOException {
        tarBundleReader = new TarArchiveBundleReader();
        tarBundleWriter = new TarArchiveBundleWriter();
        expandedBundleReader = new ExpandedArchiveBundleReader();
        expandedArchiveBundleWriter = new ExpandedArchiveBundleWriter();
        testDirectory = Files.createTempDirectory("testTarBundleRW");
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
    public void readBundleToStream() throws Exception {
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
}
