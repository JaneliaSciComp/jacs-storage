package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import org.apache.commons.compress.utils.IOUtils;
import org.hamcrest.Matchers;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.contentfilters.IDContentStreamFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

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

public class DataDirectoryBundleReaderWriterTest {

    private static final String TEST_DATA_DIRECTORY1 = "src/test/resources/testdata/bundletransfer";

    private Path testDirectory;
    private Path testDataDir;
    private DataDirectoryBundleReader dataDirectoryBundleReader;
    private DataDirectoryBundleWriter dataDirectoryBundleWriter;

    @Before
    public void setUp() throws IOException {
        ContentStreamFilterProvider contentStreamFilterProvider = Mockito.mock(ContentStreamFilterProvider.class);
        Mockito.when(contentStreamFilterProvider.getContentStreamFilter(ArgumentMatchers.any(ContentFilterParams.class)))
                .thenReturn(new IDContentStreamFilter());
        dataDirectoryBundleReader = new DataDirectoryBundleReader(contentStreamFilterProvider);
        dataDirectoryBundleWriter = new DataDirectoryBundleWriter();
        testDirectory = Files.createTempDirectory("DataDirectoryBundleReaderWriterTest");
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
            long nReadBytes = dataDirectoryBundleReader.readBundle(testDataDir.toString(), new ContentFilterParams(), testOutputStream);
            testOutputStream.close();
            testOutputStream = null;
            testInputStream = new BufferedInputStream(new FileInputStream(testFilePath.toFile()));
            long nWrittenBytes = dataDirectoryBundleWriter.writeBundle(testInputStream, testExpandedPath.toString());
            assertEquals(nReadBytes, nWrittenBytes);
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
        long nBytes = dataDirectoryBundleReader.readBundle(testDataPath.toString(), new ContentFilterParams(), output);
        assertThat(nBytes, Matchers.equalTo((long) testDataBytes.length));
        assertThat(output.toByteArray().length, Matchers.greaterThanOrEqualTo(testDataBytes.length));
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
            List<DataNodeInfo> nodeList = dataDirectoryBundleReader.listBundleContent(testDataDir.toString(), null, depth);
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
                new TestData("d_1_2/d_1_2_1", 1, ImmutableList.of("d_1_2/d_1_2_1", "d_1_2/d_1_2_1/f_1_2_1_1")),
                new TestData("d_1_2", 1, ImmutableList.of("d_1_2", "d_1_2/d_1_2_1", "d_1_2/f_1_2_1")),
                new TestData("d_1_2", 2, ImmutableList.of("d_1_2", "d_1_2/d_1_2_1", "d_1_2/f_1_2_1", "d_1_2/d_1_2_1/f_1_2_1_1")),
                new TestData("d_1_3", 2, ImmutableList.of("d_1_3", "d_1_3/f_1_3_1", "d_1_3/f_1_3_2"))
        );
        for (TestData td : testData) {
            List<DataNodeInfo> nodeList = dataDirectoryBundleReader.listBundleContent(testDataDir.toString(), td.entryName, td.depth);
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
            dataDirectoryBundleReader.readBundle(testDataDir + "/" + td, new ContentFilterParams(), referenceOutputStream);
            ByteArrayOutputStream testDataEntryStream = new ByteArrayOutputStream();
            dataDirectoryBundleReader.readDataEntry(testDataDir.toString(), td, new ContentFilterParams(), testDataEntryStream);
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
            dataDirectoryBundleReader.readDataEntry(testDataDir.toString(), td, new ContentFilterParams(), testDataEntryStream);
            assertArrayEquals("Expected condition not met for " + td, referenceOutputStream.toByteArray(), testDataEntryStream.toByteArray());
        }
    }

    @Test
    public void bundleReadFailureBecauseSourceIsMissing() {
        Path missingDataPath = testDataDir.resolve("missing");
        assertThatThrownBy(() -> dataDirectoryBundleReader.readBundle(missingDataPath.toString(), new ContentFilterParams(), new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No path found for " + missingDataPath);
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
            long size = dataDirectoryBundleWriter.createDirectoryEntry(testDataDir.toString(), td);
            assertTrue(size > 0);
        }
        List<String> tarEntryNames = dataDirectoryBundleReader.listBundleContent(testDataDir.toString(), "", 10).stream()
                .map(ni -> ni.getNodeRelativePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td, is(in(tarEntryNames)));
        });
    }

    @Test
    public void tryToCreateDirectoryEntryWhenEntryExist() throws IOException {
        String testData = "d_1_1";
        assertThatThrownBy(() -> dataDirectoryBundleWriter.createDirectoryEntry(testDataDir.toString(), testData))
                .isInstanceOf(DataAlreadyExistException.class)
                .hasMessage("Entry " + testDataDir.resolve(testData) + " already exists");
    }

    @Test
    public void tryToCreateDirectoryEntryWhenParentEntriesNotFound() {
        List<String> testData = ImmutableList.of(
                "d_1_5/d_1_5_2",
                "d_1_6/d_1_6_1/d_1_6_1_1"
        );
        for (String td : testData) {
            long size = dataDirectoryBundleWriter.createDirectoryEntry(testDataDir.toString(), td);
            assertTrue(size > 0);
        }
        List<String> tarEntryNames = dataDirectoryBundleReader.listBundleContent(testDataDir.toString(), "", 10).stream()
                .map(ni -> ni.getNodeRelativePath())
                .collect(Collectors.toList());
        testData.forEach(td -> {
            assertThat(td, is(in(tarEntryNames)));
        });
    }

    @Test
    public void tryToCreateDirectoryEntryWhenNoParentExistButNotADirectory() throws IOException {
        String testData = "d_1_3/f_1_3_1/d_1_3_1_1";
        assertThatThrownBy(() -> dataDirectoryBundleWriter.createDirectoryEntry(testDataDir.toString(), testData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parent entry found for " + testDataDir.resolve(testData) + " but it is not a directory");
    }

    @Test
    public void tryToCreateFileEntryWhenEntryExist() throws IOException {
        String testData = "d_1_1/f_1_1_1";
        assertThatThrownBy(() -> dataDirectoryBundleWriter.createFileEntry(testDataDir.toString(), testData, new FileInputStream(testDataDir.resolve("d_1_1/f_1_1_1").toFile())))
                .isInstanceOf(DataAlreadyExistException.class)
                .hasMessage("Entry " + testDataDir.resolve(testData) + " already exists");
    }

}
