package org.janelia.jacsstorage.io;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.core.IsNot;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.collection.IsIn.in;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TarBundleReaderWriterTest {

    private static final String TEST_DATA_DIRECTORY = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private Path testTarFile;
    private ExpandedArchiveBundleReader expandedArchiveBundleReader;
    private TarArchiveBundleReader tarBundleReader;
    private TarArchiveBundleWriter tarBundleWriter;

    @Before
    public void setUp() throws IOException {
        expandedArchiveBundleReader = new ExpandedArchiveBundleReader();
        tarBundleReader = new TarArchiveBundleReader();
        tarBundleWriter = new TarArchiveBundleWriter();
        testDirectory = Files.createTempDirectory("TarBundleReaderWriterTest");
        testTarFile = createTestTarFile();
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

    private Path createTestTarFile() throws IOException {
        Path testTarFilePath = testDirectory.resolve("tarBundle.tar");
        try(OutputStream testOutputStream = new FileOutputStream(testTarFilePath.toFile())) {
            expandedArchiveBundleReader.readBundle(TEST_DATA_DIRECTORY, testOutputStream);
        }
        return testTarFilePath;
    }

    @Test
    public void listContentTree() {
        List<List<String>> expectedResults = ImmutableList.of(
                ImmutableList.of("/"),
                ImmutableList.of("f_1_1", "d_1_1/", "f_1_2", "d_1_2/", "d_1_3/", "f_1_3"),
                ImmutableList.of("d_1_1/f_1_1_1", "d_1_2/d_1_2_1/", "d_1_2/f_1_2_1", "d_1_3/f_1_3_1", "d_1_3/f_1_3_2"),
                ImmutableList.of("d_1_2/d_1_2_1/f_1_2_1_1"),
                ImmutableList.of(),
                ImmutableList.of()
        );
        for (int depth = 1; depth < expectedResults.size(); depth++) {
            List<DataNodeInfo> nodeList = tarBundleReader.listBundleContent(testTarFile.toString(), null, depth);
            List<String> currentExpectedResults = IntStream.rangeClosed(0, depth)
                    .mapToObj(i -> expectedResults.get(i))
                    .flatMap(l -> l.stream())
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(currentExpectedResults, nodeList.stream().map(ni -> ni.getNodeRelativePath()).sorted().collect(Collectors.toList()));
        }
    }

    @Test
    public void listContentSubTree() {
        class TestData {
            final String entryName;
            final int depth;
            final List<String> expectedResults;

            public TestData(String entryName, int depth, List<String> expectedResults) {
                this.entryName = entryName;
                this.depth = depth;
                this.expectedResults = expectedResults;
            }
        }
        List<TestData> testData = ImmutableList.of(
                new TestData("f_1_1", 0, ImmutableList.of("f_1_1")),
                new TestData("d_1_2/d_1_2_1", 1, ImmutableList.of("d_1_2/d_1_2_1/", "d_1_2/d_1_2_1/f_1_2_1_1")),
                new TestData("d_1_2", 1, ImmutableList.of("d_1_2/", "d_1_2/d_1_2_1/", "d_1_2/f_1_2_1")),
                new TestData("d_1_2", 2, ImmutableList.of("d_1_2/", "d_1_2/d_1_2_1/", "d_1_2/f_1_2_1", "d_1_2/d_1_2_1/f_1_2_1_1")),
                new TestData("d_1_3", 2, ImmutableList.of("d_1_3/", "d_1_3/f_1_3_1", "d_1_3/f_1_3_2"))
        );
        for (TestData td : testData) {
            List<DataNodeInfo> nodeList = tarBundleReader.listBundleContent(testTarFile.toString(), td.entryName, td.depth);
            List<String> currentExpectedResults = td.expectedResults.stream().sorted().collect(Collectors.toList());
            assertEquals("For entry " + td.entryName + " depth " + td.depth, currentExpectedResults, nodeList.stream().map(ni -> ni.getNodeRelativePath()).sorted().collect(Collectors.toList()));
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
            expandedArchiveBundleReader.readBundle(TEST_DATA_DIRECTORY + "/" + td, referenceOutputStream);
            ByteArrayOutputStream testDataEntryStream = new ByteArrayOutputStream();
            tarBundleReader.readDataEntry(testTarFile.toString(), td, testDataEntryStream);
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
            Files.copy(Paths.get(TEST_DATA_DIRECTORY, td), referenceOutputStream);
            ByteArrayOutputStream testDataEntryStream = new ByteArrayOutputStream();
            tarBundleReader.readDataEntry(testTarFile.toString(), td, testDataEntryStream);
            assertArrayEquals("Expected condition not met for " + td, referenceOutputStream.toByteArray(), testDataEntryStream.toByteArray());
        }
    }

    @Test
    public void readWriteCheck() throws Exception {
        Path testReaderPath = testDirectory.resolve("readArchiveBundle");
        Path testWriterPath = testDirectory.resolve("writtenArchiveBundle");
        OutputStream testOutputStream = null;
        InputStream testInputStream = null;
        try {
            testOutputStream = new FileOutputStream(testReaderPath.toFile());
            long nReadBytes = tarBundleReader.readBundle(testTarFile.toString(), testOutputStream);
            testOutputStream.close();
            testOutputStream = null;
            testInputStream = new BufferedInputStream(new FileInputStream(testReaderPath.toFile()));
            long nWrittenBytes = tarBundleWriter.writeBundle(testInputStream, testWriterPath.toString());
            assertEquals(nReadBytes, nWrittenBytes);
        } finally {
            IOUtils.closeQuietly(testOutputStream);
            IOUtils.closeQuietly(testInputStream);
        }
    }

    @Test
    public void invalidDataEntry() throws IOException {
        String testEntryName = "not-present";
        assertThatThrownBy(() -> tarBundleReader.readDataEntry(testTarFile.toString(), testEntryName, new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No entry " + testEntryName + " found under " + testTarFile.toString());
    }

    @Test
    public void bundleReadFailureBecauseSourceIsMissing() {
        Path testDataPath = Paths.get(TEST_DATA_DIRECTORY, "missing.tar");

        assertThatThrownBy(() -> tarBundleReader.readBundle(testDataPath.toString(), new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No file found for " + testDataPath.toString());
    }

    @Test
    public void createDirectoryEntry() {
        List<String> testData = ImmutableList.of(
                "d_1_5",
                "d_1_1/d_1_1_1",
                "d_1_2/d_1_2_2",
                "d_1_2/d_1_2_1/d_1_2_1_1",
                "d_1_3/d_1_3_1",
                "d_1_5/d_1_5_1"
        );
        for (String td : testData) {
            long size = tarBundleWriter.createDirectoryEntry(testTarFile.toString(), td);
            assertTrue(size == TarConstants.DEFAULT_RCDSIZE);
        }
        List<String> tarEntryNames = tarBundleReader.listBundleContent(testTarFile.toString(), null, 10).stream()
                .map(ni -> ni.getNodeRelativePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td + "/", is(in(tarEntryNames)));
        });
    }

    @Test
    public void tryToCreateDirectoryEntryWhenEntryExist() throws IOException {
        String testData = "d_1_1";
        assertThatThrownBy(() -> tarBundleWriter.createDirectoryEntry(testTarFile.toString(), testData))
                .isInstanceOf(DataAlreadyExistException.class)
                .hasMessage("Entry " + testData + " already exists");
    }

    @Test
    public void tryToCreateDirectoryEntryWhenParentEntriesNotFound() {
        List<Pair<String, Long>> testData = ImmutableList.of(
                ImmutablePair.of("d_1_5/d_1_5_2", (long) 2 * TarConstants.DEFAULT_RCDSIZE),
                ImmutablePair.of("d_1_6/d_1_6_1/d_1_6_1_1", (long) 3 * TarConstants.DEFAULT_RCDSIZE)
        );
        for (Pair<String, Long> td : testData) {
            long size = tarBundleWriter.createDirectoryEntry(testTarFile.toString(), td.getLeft());
            assertTrue(size == td.getRight());
        }
        List<String> tarEntryNames = tarBundleReader.listBundleContent(testTarFile.toString(), null, 10).stream()
                .map(ni -> ni.getNodeRelativePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td.getLeft() + "/", is(in(tarEntryNames)));
        });
    }

    @Test
    public void tryToCreateDirectoryEntryWhenParentExistButNotADirectory() {
        String testData = "d_1_3/f_1_3_1/d_1_3_1_1";
        assertThatThrownBy(() -> tarBundleWriter.createDirectoryEntry(testTarFile.toString(), testData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parent entry found for " + testData + " but it is not a directory");
    }

    @Test
    public void createFileEntry() throws FileNotFoundException {
        List<String> testData = ImmutableList.of(
                "d_1_1/f_1_1_2",
                "d_1_2/d_1_2_1/f_1_2_1_2"
        );
        for (String td : testData) {
            long size = tarBundleWriter.createFileEntry(testTarFile.toString(), td, new FileInputStream(Paths.get(TEST_DATA_DIRECTORY, "d_1_1/f_1_1_1").toFile()));
            assertTrue(size > 2 * TarConstants.DEFAULT_RCDSIZE);
        }
        List<String> tarEntryNames = tarBundleReader.listBundleContent(testTarFile.toString(), null, 10).stream()
                .map(ni -> ni.getNodeRelativePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td, is(in(tarEntryNames)));
        });
    }

    @Test
    public void tryToCreateFileEntryWhenEntryExist() throws IOException {
        String testData = "d_1_1/f_1_1_1";
        assertThatThrownBy(() -> tarBundleWriter.createFileEntry(testTarFile.toString(), testData, new FileInputStream(Paths.get(TEST_DATA_DIRECTORY, "d_1_1/f_1_1_1").toFile())))
                .isInstanceOf(DataAlreadyExistException.class)
                .hasMessage("Entry " + testData + " already exists");
    }

    @Test
    public void deleteDataEntry() throws IOException {
        List<String> testData = ImmutableList.of(
                "f_1_1",
                "f_1_2",
                "f_1_3",
                "d_1_1/",
                "d_1_2/d_1_2_1/"
        );
        for (String td : testData) {
            long length = tarBundleWriter.deleteEntry(testTarFile.toString(), td);
            assertTrue("Expected deleted length to be gt 0 for " + td, length > 0);
            List<String> tarEntryNames = tarBundleReader.listBundleContent(testTarFile.toString(), null, 10).stream()
                    .map(ni -> ni.getNodeRelativePath())
                    .collect(Collectors.toList());
            assertThat(td, IsNot.not(in(tarEntryNames)));
        }
    }

}
