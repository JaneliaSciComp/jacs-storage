package org.janelia.jacsstorage.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class S3StorageServiceTest {

    @Test
    public void retrieveSingleFileContentFromS3Endpoint() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "scicompsoft-public",
                "https://s3.us-east-1.lyvecloud.seagate.com",
                "us-east-1",
                "NNQ20KNJ2YCWWMPE",
                "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM"
        );
        List<ContentNode> contentNodes = storageService.listContentNodes("/scicompsoft/flynp/pipeline_info/software_versions.yml", new ContentAccessParams());
        assertEquals(1, contentNodes.size());
        String nodeContent = new String(ByteStreams.toByteArray(storageService.readContent(contentNodes.get(0).getObjectKey())));
        assertTrue(nodeContent.length() > 0);
    }

    @Test
    public void listFolderContentFromS3Endpoint() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "scicompsoft-public",
                "https://s3.us-east-1.lyvecloud.seagate.com",
                "us-east-1",
                "NNQ20KNJ2YCWWMPE",
                "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM"
        );
        List<ContentNode> contentNodes = storageService.listContentNodes("scicompsoft/flynp/pipeline_info", new ContentAccessParams());
        assertTrue(contentNodes.size() > 0);
    }

    @Test
    public void retrieveSingleFileFromS3() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "janelia-neuronbridge-data-dev",
                null,
                "us-east-1",
                null,
                null
        );
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentAccessParams()
                        .setEntryNamePattern("config.json")
                        .setMaxDepth(1));
        assertTrue(nodes.size() == 1);
        String nodeContent = new String(ByteStreams.toByteArray(storageService.readContent(nodes.get(0).getObjectKey())));
        assertNotNull(nodeContent);
    }

    @Test
    public void retrieveSelectedFilesFromS3() {
        S3StorageService storageService = new S3StorageService(
                "janelia-neuronbridge-data-dev",
                null,
                "us-east-1",
                null,
                null
        );
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentAccessParams()
                        .addSelectedEntry("config.json")
                        .addSelectedEntry("DATA_NOTES.md")
                        .setMaxDepth(1));
        assertEquals(2L, nodes.size());
    }

    @Test
    public void listContentOnPublicBucket() {
        S3StorageService storageService = new S3StorageService(
                "janelia-mouselight-imagery",
                null,
                "us-east-1",
                null,
                null
        );
        class TestData {
            final String testName;
            final String contentLocation;
            final int depth;
            final int offset;
            final boolean directoriesOnly;
            final int expectedNodes;

            TestData(String testName, String contentLocation, int depth, int offset, boolean directoriesOnly, int expectedNodes) {
                this.testName = testName;
                this.contentLocation = contentLocation;
                this.depth = depth;
                this.offset = offset;
                this.directoriesOnly = directoriesOnly;
                this.expectedNodes = expectedNodes;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("Test dirsOnly, depth 0", "images/2021-10-19", 0, 0, true, 0),
                new TestData("Test dirsOnly, depth 0, ending /", "images/2021-10-19/", 0, 0, true, 0),
                new TestData("Test dirsOnly, depth 1", "images/2021-10-19", 1, 0, true, 2),
                new TestData("Test dirsOnly, depth 1, ending /", "images/2021-10-19/", 1, 0, true, 2),
                new TestData("Test depth 1, offset 0", "images/2021-10-19", 1, 0, false, 6),
                new TestData("Test depth 1, offset 0, / ending", "images/2021-10-19/", 1, 0, false, 6),
                new TestData("Test depth 1, offset 1", "images/2021-10-19", 1, 1, false, 5),
                new TestData("Test depth 1, offset 1, / ending", "images/2021-10-19/", 1, 1, false, 5),
                new TestData("Test depth 1, offset 2", "images/2021-10-19", 1, 2, false, 4),
                new TestData("Test depth 1, offset 2, / ending", "images/2021-10-19/", 1, 2, false, 4),
                new TestData("Test depth 2, offset 0","images/2021-10-19", 2, 0, false, 15),
                new TestData("Test depth 2, offset 3","images/2021-10-19", 2, 3, false, 12),
                new TestData("Test depth 2, offset 4","images/2021-10-19", 2, 4, false, 10),
                new TestData("Test exact match","images/2021-10-19/transform.txt", 2, 0, false, 1),
                // be careful of this case - when one asks for an exact match with an offset > 0
                new TestData("Test exact match bad offset","images/2021-10-19/transform.txt", 2, 1, false, 0),
        };
        for (TestData td : testData) {
            List<ContentNode> nodes = storageService.listContentNodes(td.contentLocation,
                    new ContentAccessParams()
                            .setMaxDepth(td.depth)
                            .setStartEntryIndex(td.offset)
                            .setDirectoriesOnly(td.directoriesOnly)
            );
            assertEquals(td.testName, td.expectedNodes, nodes.size());
        }
    }

    /**
     * I only run this test from the IDE to check open buckets from a different region
     */
    @Test
    public void listContentOnPublicBucketFromDifferentRegion() {
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
        TestData[] testData = new TestData[] {
                new TestData(
                        "aind-msma-morphology-data",
                        "us-west-2", // region must match if credentials are available
                        "segmentation/exaSPIM_653159_zarr/",
                        1, 0, 9),
                new TestData(
                        "aind-open-data",
                        "us-west-2", // region must match if credentials are available
                        "/exaSPIM_653158_2023-06-01_20-41-38_fusion_2023-06-12_11-58-05/",
                        1, 0, 7),
        };
        for (TestData td : testData) {
            S3StorageService storageService = new S3StorageService(
                    td.bucket,
                    null,
                    td.region,
                    null,
                    null
            );
            List<ContentNode> nodes = storageService.listContentNodes(td.contentLocation,
                    new ContentAccessParams()
                            .setMaxDepth(td.depth)
                            .setStartEntryIndex(td.offset)
            );
            assertEquals(td.expectedNodes, nodes.size());
        }
    }

    @Test
    public void checkAccessRoot() {
        S3StorageService storageService = new S3StorageService(
                "janelia-neuronbridge-data-dev",
                null,
                "us-east-1",
                null,
                null
        );
        assertTrue(storageService.canAccess(""));
    }

    @Test
    public void writeAndDeleteContentOnS3() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "janelia-neuronbridge-data-dev",
                null,
                "us-east-1",
                null,
                null
        );
        String testContent = "This is some test content";
        long l = storageService.writeContent("myTest.txt", new ByteArrayInputStream(testContent.getBytes()));
        assertTrue(l == testContent.length());
        List<ContentNode> nodesAfterWrite = storageService.listContentNodes("myTest.txt", new ContentAccessParams());
        assertTrue(nodesAfterWrite.size() == 1);
        assertTrue(storageService.canAccess("myTest.txt"));
        String nodeContent = new String(ByteStreams.toByteArray(storageService.readContent(nodesAfterWrite.get(0).getObjectKey())));
        assertEquals(testContent, nodeContent);
        storageService.deleteContent("myTest.txt");
        List<ContentNode> nodesAfterDelete = storageService.listContentNodes("myTest.txt", new ContentAccessParams());
        assertTrue(nodesAfterDelete.size() == 0);
    }

    @Test
    public void retrievePrefixFromS3() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "janelia-neuronbridge-data-dev",
                null,
                "us-east-1",
                null,
                null
        );
        ByteArrayOutputStream testDataStream = new ByteArrayOutputStream();
        List<ContentNode> contentNodes = storageService.listContentNodes("v3_3_0/schemas", new ContentAccessParams());
        for (ContentNode n : contentNodes) {
            if (n.isNotCollection()) {
                try (InputStream nodeContentStream = storageService.readContent(n.getObjectKey())) {
                    ByteStreams.copy(nodeContentStream, testDataStream);
                }
            }
        }
        String testDataContent = testDataStream.toString();
        assertNotNull(testDataContent);
    }

}
