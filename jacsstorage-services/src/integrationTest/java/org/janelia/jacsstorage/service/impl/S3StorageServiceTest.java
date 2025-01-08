package org.janelia.jacsstorage.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentAccessProvider;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.impl.contenthandling.DirectContentAccess;
import org.janelia.jacsstorage.service.impl.contenthandling.SimpleMetadataReader;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;
import org.janelia.jacsstorage.service.s3.impl.S3AdapterProviderImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Tag("s3Access")
public class S3StorageServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(S3StorageServiceTest.class);

    private static S3AdapterProvider s3AdapterProvider;

    @BeforeAll
    public static void setUp() {
        s3AdapterProvider = new S3AdapterProviderImpl();
    }

    @Test
    public void retrieveSingleFileContentFromS3Endpoint() throws IOException {
        String accessKey = System.getenv("SEAGATE_AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("SEAGATE_AWS_SECRET_KEY_ID");
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter("scicompsoft-public",
                        "https://s3.us-east-1.lyvecloud.seagate.com",
                        JADEOptions.create()
                                .setAWSRegion("us-east-1")
                                .setAccessKey(accessKey)
                                .setSecretKey(secretKey)
                                .setPathStyleBucket(true)),
                false);
        assertTrue(storageService.canAccess("scicompsoft/flynp/pipeline_info/software_versions.yml"));
        InputStream contentStream = storageService.getContentInputStream("scicompsoft/flynp/pipeline_info/software_versions.yml");
        String content = new String(ByteStreams.toByteArray(contentStream));
        assertTrue(content.length() > 0);
        List<ContentNode> contentNodes = storageService.listContentNodes("/scicompsoft/flynp/pipeline_info/software_versions.yml", new ContentAccessParams());
        assertEquals(1, contentNodes.size());
        String nodeContent = new String(ByteStreams.toByteArray(storageService.getContentInputStream(contentNodes.get(0).getObjectKey())));
        assertTrue(nodeContent.length() > 0);
    }

    @Test
    public void listFolderContentFromS3Endpoint() {
        String accessKey = System.getenv("NRS_AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("NRS_AWS_SECRET_KEY_ID");
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "scicompsoft-data-public",
                        "https://s3nrsext.int.janelia.org",
                        JADEOptions.create()
                                .setAWSRegion("us-east-1")
                                .setAccessKey(accessKey)
                                .setSecretKey(secretKey)
                                .setPathStyleBucket(true)),
                false);
        List<ContentNode> contentNodes = storageService.listContentNodes("", new ContentAccessParams());
        assertFalse(contentNodes.isEmpty());
    }

    @Test
    public void retrieveSingleFileFromS3() throws IOException {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentAccessParams()
                        .setEntryNamePattern("config.json")
                        .setMaxDepth(1));
        assertTrue(nodes.size() == 1);
        String nodeContent = new String(ByteStreams.toByteArray(storageService.getContentInputStream(nodes.get(0).getObjectKey())));
        assertNotNull(nodeContent);
    }

    @Test
    public void retrieveExistingObjectFromS3() {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        ContentNode n = storageService.getObjectNode("v3_3_0/config.json");
        assertNotNull(n);
    }

    @Test
    public void retrieveSelectedFilesFromS3() {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentAccessParams()
                        .addSelectedEntry("config.json")
                        .addSelectedEntry("DATA_NOTES.md")
                        .setMaxDepth(1));
        assertEquals(2L, nodes.size());
    }

    @Test
    public void listContentOnPublicBucket() {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-mouselight-imagery",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        class TestData {
            final String testName;
            final String contentLocation;
            final String pattern;
            final List<String> selectedEntries;
            final int depth;
            final int offset;
            final boolean directoriesOnly;
            final int expectedNodes;

            TestData(String testName, String contentLocation,
                     String pattern,
                     List<String> selectedEntries,
                     int depth, int offset,
                     boolean directoriesOnly,
                     int expectedNodes) {
                this.testName = testName;
                this.contentLocation = contentLocation;
                this.pattern = pattern;
                this.selectedEntries = selectedEntries == null ? Collections.emptyList() : selectedEntries;
                this.depth = depth;
                this.offset = offset;
                this.directoriesOnly = directoriesOnly;
                this.expectedNodes = expectedNodes;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData("Test dirsOnly on root", "", null, null, 1, 0, true, 8),
                new TestData("Test dirsOnly on root, using '/'", "/", null, null, 1, 0, true, 8),

                new TestData("Test dirsOnly, depth 0", "images/2021-10-19", null, null, 0, 0, true, 1),
                new TestData("Test dirsOnly, depth 0, ending /", "images/2021-10-19/", null, null, 0, 0, true, 1),

                new TestData("Test dirsOnly, depth 1", "images/2021-10-19", null, null, 1, 0, true, 2),
                new TestData("Test dirsOnly, depth 1, ending /", "images/2021-10-19/", null, null, 1, 0, true, 2),

                new TestData("Test dirs and objects on root", "", null, null, 1, 0, false, 9),
                new TestData("Test depth 1, offset 0", "images/2021-10-19", null, null, 1, 0, false, 6),
                new TestData("Test depth 1, offset 0, / ending", "images/2021-10-19/", null, null, 1, 0, false, 6),

                new TestData("Test depth 1, offset 1", "images/2021-10-19", null, null, 1, 1, false, 5),
                new TestData("Test depth 1, offset 1, / ending", "images/2021-10-19/", null, null, 1, 1, false, 5),

                new TestData("Test depth 1, offset 2", "images/2021-10-19", null, null, 1, 2, false, 4),
                new TestData("Test depth 1, offset 2, / ending", "images/2021-10-19/", null, null, 1, 2, false, 4),

                new TestData("Test depth 2, offset 0", "images/2021-10-19", null, null, 2, 0, false, 15),
                new TestData("Test depth 2, offset 3", "images/2021-10-19", null, null, 2, 3, false, 12),
                new TestData("Test depth 2, offset 4", "images/2021-10-19", null, null, 2, 4, false, 11),

                new TestData("Test exact match", "images/2021-10-19/transform.txt", null, null, 2, 0, false, 1),
                // be careful of this case - when one asks for an exact match with an offset > 0
                new TestData("Test exact match bad offset", "images/2021-10-19/transform.txt", null, null, 2, 1, false, 0),
                new TestData("Test selected entries", "images/2021-10-19", null, Arrays.asList("default.0.tif", "default.1.tif"), 1, 0, false, 2),
                new TestData("Test selected entries (path ending /)", "images/2021-10-19/", null, Arrays.asList("default.0.tif", "default.1.tif"), 1, 0, false, 2),
                new TestData("Test pattern", "images/2021-10-19", "default.*tif", Collections.emptyList(), 1, 0, false, 2),
                new TestData("Test pattern (path ending /)", "images/2021-10-19/", "default.0.tif", Collections.emptyList(), 1, 0, false, 1),
        };
        for (TestData td : testData) {
            List<ContentNode> nodes = storageService.listContentNodes(td.contentLocation,
                    new ContentAccessParams()
                            .setEntryNamePattern(td.pattern)
                            .addSelectedEntries(td.selectedEntries)
                            .setMaxDepth(td.depth)
                            .setStartEntryIndex(td.offset)
                            .setDirectoriesOnly(td.directoriesOnly)
            );
            assertEquals(td.expectedNodes, nodes.size(), td.testName);
        }
    }

    @Test
    public void listContentFromPublicBucketInDifferentRegion() {
        class TestData {
            final String bucket;
            final String region;
            final String contentLocation;
            final int depth;
            final int offset;
            final int expectedNodes;

            TestData(String bucket, String region, String contentLocation, int depth, int offset, int expectedNodes) {
                this.bucket = bucket;
                this.region = region;
                this.contentLocation = contentLocation;
                this.depth = depth;
                this.offset = offset;
                this.expectedNodes = expectedNodes;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr", // no '/' at the end of the prefix
                        1, 0, 9),
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/", // prefix terminates with '/'
                        1, 0, 9),
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/swcs_1.zip/",
                        1, 0, 0),
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/swcs_1.zip",
                        1, 0, 1),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "/exaSPIM_653158_2023-06-01_20-41-38_fusion_2023-06-12_11-58-05",
                        1, 0, 7),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "/exaSPIM_653158_2023-06-01_20-41-38_fusion_2023-06-12_11-58-05/",
                        1, 0, 7),
        };
        for (TestData td : testData) {
            ContentStorageService storageService = getS3StorageService(
                    s3AdapterProvider.getS3Adapter(
                            td.bucket,
                            null,
                            JADEOptions.create()
                                    .setAWSRegion(td.region)
                                    .setAsyncAccess(true)),
                    true);
            List<ContentNode> nodes = storageService.listContentNodes(td.contentLocation,
                    new ContentAccessParams()
                            .setMaxDepth(td.depth)
                            .setStartEntryIndex(td.offset)
            );
            assertEquals(td.expectedNodes, nodes.size());
        }
    }

    @Test
    public void readSingleObjectContentFromPublicBucketInDifferentRegion() {
        class TestData {
            final String bucket;
            final String region;
            final String contentLocation;
            final int expectedSize;

            TestData(String bucket, String region, String contentLocation, int expectedSize) {
                this.bucket = bucket;
                this.region = region;
                this.contentLocation = contentLocation;
                this.expectedSize = expectedSize;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "exaSPIM_653158_2023-06-01_20-41-38_fusion_2023-06-12_11-58-05/fused.zarr/4/0/0/1/3/6",
                        7505274),
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/swcs_0.zip",
                        176985681),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "exaSPIM_731886_2024-08-30_13-43-58_flatfield-correction_2024-09-09_10-42-55_fusion_2024-09-09_15-03-51/fused.zarr/0/0/0/0/0/1",
                        7823617)
        };
        for (TestData td : testData) {
            ContentStorageService storageService = getS3StorageService(
                    s3AdapterProvider.getS3Adapter(
                    td.bucket,
                    null,
                    JADEOptions.create()
                            .setAWSRegion(td.region)
                            .setAsyncAccess(true)),
                    true);
            long startTime = System.currentTimeMillis();
            ByteArrayOutputStream retrievedStream = new ByteArrayOutputStream();
            long nbytes = storageService.streamContentToOutput(td.contentLocation, retrievedStream);
            assertEquals(td.expectedSize, nbytes);
            double accessTime = (System.currentTimeMillis() - startTime) / 1000.;
            LOG.info("Finished sync processing stream after {} secs", accessTime);
            retrievedStream.reset();
        }
    }

    /**
     * Reads a single object or an entire prefix from a public bucket in a different region.
     */
    @Test
    public void readSingleOrMultipleObjectsContentFromPublicBucketInDifferentRegion() {
        class TestData {
            final String bucket;
            final String region;
            final String contentLocation;
            final boolean asyncAccess;
            final int expectedNodes;
            final long expectedSize;

            TestData(String bucket, String region, String contentLocation, boolean asyncAccess, int expectedNodes, long expectedSize) {
                this.bucket = bucket;
                this.region = region;
                this.contentLocation = contentLocation;
                this.asyncAccess = asyncAccess;
                this.expectedNodes = expectedNodes;
                this.expectedSize = expectedSize;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "exaSPIM_653158_2023-06-01_20-41-38_fusion_2023-06-12_11-58-05/fused.zarr/4/0/0/1/3/6",
                        true,
                        1,
                        7505274),
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/swcs_0.zip",
                        true,
                        1,
                        176985681L),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "exaSPIM_731886_2024-08-30_13-43-58_flatfield-correction_2024-09-09_10-42-55_fusion_2024-09-09_15-03-51/fused.zarr/0/0/0/0/0/1",
                        true,
                        1,
                        7823617),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "exaSPIM_731886_2024-08-30_13-43-58_flatfield-correction_2024-09-09_10-42-55_fusion_2024-09-09_15-03-51/fused.zarr/0/0/0/0/0",
                        true,
                        5,
                        30748672L)
        };
        for (TestData td : testData) {
            S3Adapter s3Adapter = s3AdapterProvider.getS3Adapter(
                    td.bucket,
                    null,
                    JADEOptions.create()
                            .setAWSRegion(td.region)
                            .setAsyncAccess(td.asyncAccess));
            ContentStorageService storageService = getS3StorageService(s3Adapter, td.asyncAccess);
            long startTime = System.currentTimeMillis();
            ContentAccessParams contentAccessParams = new ContentAccessParams().setEntriesCount(td.expectedNodes);
            List<ContentNode> contentNodes = storageService.listContentNodes(td.contentLocation, contentAccessParams);
            assertEquals(td.expectedNodes, contentNodes.size());
            contentNodes.sort((n1, n2) -> ComparatorUtils.naturalCompare(n1.getObjectKey(), n2.getObjectKey(), true)); // sort by key
            ContentGetter contentGetter = new ContentGetterImpl(
                    storageService,
                    new ContentAccessProvider() {
                        @Override
                        public ContentAccess getContentFilter(ContentAccessParams contentAccessParams) {
                            return new DirectContentAccess(false);
                        }

                        @Override
                        public ContentMetadataReader getContentMetadataReader(String mimeType) {
                            return new SimpleMetadataReader();
                        }
                    },
                    contentNodes,
                    contentAccessParams
            );

            ByteArrayOutputStream retrievedStream = new ByteArrayOutputStream();
            long nbytes = contentGetter.streamContent(retrievedStream);
            assertEquals(td.expectedSize, nbytes);
            double accessTime = (System.currentTimeMillis() - startTime) / 1000.;
            LOG.info("Finished sync processing stream after {} secs", accessTime);
        }
    }

    @Test
    public void checkAccess() {
        class TestData {
            final String testName;
            final String location;
            final boolean expectedResult;

            TestData(String testName, String location, boolean expectedResult) {
                this.testName = testName;
                this.location = location;
                this.expectedResult = expectedResult;
            }
        }
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        TestData[] testData = new TestData[]{
                new TestData("Access root", "", true),
                new TestData("Access file on root", "current.txt", true),
                new TestData("Access prefix", "v3_4_0", true),
                new TestData("Access prefix ending in /", "v3_4_0/", true),
                new TestData("Access bad file", "missing", false),
                new TestData("Access incomplete prefix", "v3", true),
                new TestData("Access bad prefix", "v3/", false),
        };
        for (TestData td : testData) {
            assertEquals(td.expectedResult, storageService.canAccess(td.location), td.testName);
        }
    }

    @Test
    public void writeAndDeleteContentOnS3() throws IOException {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        String testContent = "This is some test content";
        long l = storageService.writeContent("myTest.txt", new ByteArrayInputStream(testContent.getBytes()));
        assertTrue(l == testContent.length());
        List<ContentNode> nodesAfterWrite = storageService.listContentNodes("myTest.txt", new ContentAccessParams());
        assertTrue(nodesAfterWrite.size() == 1);
        assertTrue(storageService.canAccess("myTest.txt"));
        String nodeContent = new String(ByteStreams.toByteArray(storageService.getContentInputStream(nodesAfterWrite.get(0).getObjectKey())));
        assertEquals(testContent, nodeContent);
        storageService.deleteContent("myTest.txt");
        List<ContentNode> nodesAfterDelete = storageService.listContentNodes("myTest.txt", new ContentAccessParams());
        assertTrue(nodesAfterDelete.size() == 0);
    }

    @Test
    public void retrievePrefixFromS3() {
        ContentStorageService storageService = getS3StorageService(
                s3AdapterProvider.getS3Adapter(
                        "janelia-neuronbridge-data-dev",
                        null,
                        JADEOptions.create()
                                .setDefaultAWSRegion("us-east-1")
                                .setDefaultAsyncAccess(true)),
                true);
        ByteArrayOutputStream testDataStream = new ByteArrayOutputStream();
        List<ContentNode> contentNodes = storageService.listContentNodes("v3_3_0/schemas", new ContentAccessParams());
        for (ContentNode n : contentNodes) {
            if (n.isNotCollection()) {
                storageService.streamContentToOutput(n.getObjectKey(), testDataStream);
            }
        }
        String testDataContent = testDataStream.toString();
        assertNotNull(testDataContent);
    }

    private ContentStorageService getS3StorageService(S3Adapter s3Adapter, boolean async) {
        return async ? new AsyncS3StorageService(s3Adapter) : new SyncS3StorageService(s3Adapter);
    }
}
