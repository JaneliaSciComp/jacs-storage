package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.utils.IOUtils;
import org.hamcrest.Matchers;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.utils.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ExpandedBundleReaderWriterTest {

    private static final String TEST_DATA_DIRECTORY1 = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private Path testDataDir;
    private ExpandedArchiveBundleReader expandedBundleReader;
    private ExpandedArchiveBundleWriter expandedArchiveBundleWriter;

    @Before
    public void setUp() throws IOException {
        expandedBundleReader = new ExpandedArchiveBundleReader();
        expandedArchiveBundleWriter = new ExpandedArchiveBundleWriter();
        testDirectory = Files.createTempDirectory("ExpandedBundleReaderWriterTest");
        testDataDir = testDirectory.resolve("tmpTestDataDir");
        PathUtils.copyFiles(Paths.get(TEST_DATA_DIRECTORY1), testDataDir);
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
        Path testFilePath = testDirectory.resolve("readBundleToStream");
        Path testExpandedPath = testDirectory.resolve("expandedArchiveBundle");
        OutputStream testOutputStream = null;
        InputStream testInputStream = null;
        try {
            testOutputStream = new FileOutputStream(testFilePath.toFile());
            TransferInfo sentInfo = expandedBundleReader.readBundle(testDataDir.toString(), testOutputStream);
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
        Path testDataPath = testDataDir.resolve("f_1_1");
        byte[] testDataBytes = Files.readAllBytes(testDataPath);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TransferInfo info = expandedBundleReader.readBundle(testDataPath.toString(), output);
        assertThat(info.getNumBytes(), Matchers.equalTo((long) testDataBytes.length));
        assertThat(output.toByteArray().length, Matchers.greaterThan(testDataBytes.length));
    }

    @Test
    public void listContentTree() {
        List<List<String>> expectedResults = ImmutableList.of(
                ImmutableList.of(""),
                ImmutableList.of("f_1_1", "d_1_1", "f_1_2", "d_1_2", "d_1_3", "f_1_3"),
                ImmutableList.of("d_1_1/f_1_1_1", "d_1_2/d_1_2_1", "d_1_2/f_1_2_1", "d_1_3/f_1_3_1", "d_1_3/f_1_3_2"),
                ImmutableList.of("d_1_2/d_1_2_1/f_1_2_1_1"),
                ImmutableList.of(),
                ImmutableList.of()
        );
        for (int depth = 1; depth < expectedResults.size(); depth++) {
            List<DataNodeInfo> nodeList = expandedBundleReader.listBundleContent(testDataDir.toString(), depth);
            List<String> currentExpectedResults = IntStream.rangeClosed(0, depth)
                    .mapToObj(i -> expectedResults.get(i))
                    .flatMap(l -> l.stream())
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(currentExpectedResults, nodeList.stream().map(ni -> ni.getNodePath()).sorted().collect(Collectors.toList()));
        }
    }

    @Test
    public void readDirectoryDataEntry() throws IOException {
        List<String> testData = ImmutableList.of(
                "",
                "d_1_1",
                "d_1_2",
                "d_1_2/d_1_2_1",
                "d_1_3"
        );
        for (String td : testData) {
            ByteArrayOutputStream referenceOutputStream = new ByteArrayOutputStream();
            expandedBundleReader.readBundle(testDataDir + "/" + td, referenceOutputStream);
            ByteArrayOutputStream testDataEntryStream = new ByteArrayOutputStream();
            expandedBundleReader.readDataEntry(testDataDir.toString(), td, testDataEntryStream);
            assertArrayEquals("Expected condition not met for " + td, referenceOutputStream.toByteArray(), testDataEntryStream.toByteArray());
        }
    }

    @Test
    public void readFileDataEntry() throws IOException {
        List<String> testData = ImmutableList.of(
                "f_1_1",
                "f_1_2",
                "f_1_3",
                "d_1_1/f_1_1_1",
                "d_1_2/f_1_2_1",
                "d_1_2/d_1_2_1/f_1_2_1_1",
                "d_1_3/f_1_3_1"
        );
        for (String td : testData) {
            ByteArrayOutputStream referenceOutputStream = new ByteArrayOutputStream();
            Files.copy(testDataDir.resolve(td), referenceOutputStream);
            ByteArrayOutputStream testDataEntryStream = new ByteArrayOutputStream();
            expandedBundleReader.readDataEntry(testDataDir.toString(), td, testDataEntryStream);
            assertArrayEquals("Expected condition not met for " + td, referenceOutputStream.toByteArray(), testDataEntryStream.toByteArray());
        }
    }

    @Test
    public void bundleReadFailureBecauseSourceIsMissing() {
        Path missingDataPath = testDataDir.resolve("missing");
        assertThatThrownBy(() -> expandedBundleReader.readBundle(missingDataPath.toString(), new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("java.lang.IllegalArgumentException: No path found for " + missingDataPath);
    }

    @Test
    public void createDirectoryEntry() throws IOException {
        List<String> testData = ImmutableList.of(
                "d_1_5",
                "d_1_1/d_1_1_1",
                "d_1_2/d_1_2_2",
                "d_1_2/d_1_2_1/d_1_2_1_1",
                "d_1_3/d_1_3_1",
                "d_1_5/d_1_5_1"
        );
        for (String td : testData) {
            long size = expandedArchiveBundleWriter.createDirectoryEntry(testDataDir.toString(), td);
            assertTrue(size > 0);
        }
        List<String> tarEntryNames = expandedBundleReader.listBundleContent(testDataDir.toString(), 10).stream()
                .map(ni -> ni.getNodePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td, isIn(tarEntryNames));
        });
    }

    @Test
    public void tryToCreateDirectoryEntryWhenEntryExist() throws IOException {
        String testData = "d_1_1";
        assertThatThrownBy(() -> expandedArchiveBundleWriter.createDirectoryEntry(testDataDir.toString(), testData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Entry " + testDataDir.resolve(testData) + " already exists");
    }

    @Test
    public void tryToCreateDirectoryEntryWhenNoParentEntryExist() throws IOException {
        String testData = "d_1_5/d_1_5_2";
        assertThatThrownBy(() -> expandedArchiveBundleWriter.createDirectoryEntry(testDataDir.toString(), testData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No parent entry found for " + testDataDir.resolve(testData));
    }

    @Test
    public void tryToCreateDirectoryEntryWhenNoParentExistButNotADirectory() throws IOException {
        String testData = "d_1_3/f_1_3_1/d_1_3_1_1";
        assertThatThrownBy(() -> expandedArchiveBundleWriter.createDirectoryEntry(testDataDir.toString(), testData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parent entry found for " + testDataDir.resolve(testData) + " but it is not a directory");
    }
}
