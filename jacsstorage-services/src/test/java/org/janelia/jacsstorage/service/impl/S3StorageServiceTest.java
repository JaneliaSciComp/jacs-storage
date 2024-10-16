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
        assertEquals(2, nodes.size());
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
            final String contentLocation;
            final int depth;
            final int offset;
            final int expectedNodes;

            TestData(String contentLocation, int depth, int offset, int expectedNodes) {
                this.contentLocation = contentLocation;
                this.depth = depth;
                this.offset = offset;
                this.expectedNodes = expectedNodes;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("images/2021-10-19", 0, 0, 4),
                new TestData("images/2021-10-19/", 0, 0, 4),
                new TestData("images/2021-10-19", 1, 0, 5),
                new TestData("images/2021-10-19", 1, 3, 2),
                new TestData("images/2021-10-19", 1, 4, 1),
                new TestData("images/2021-10-19/transform.txt", 2, 0, 1),
                // be careful of this case - when one asks for an exact match with an offset > 0
                new TestData("images/2021-10-19/transform.txt", 2, 1, 0),
        };
        for (TestData td : testData) {
            List<ContentNode> nodes = storageService.listContentNodes(td.contentLocation,
                    new ContentAccessParams()
                            .setMaxDepth(td.depth)
                            .setStartEntryIndex(td.offset)
            );
            assertEquals(td.expectedNodes, nodes.size());
        }
    }

    /**
     * I only run this test from the IDE to check open buckets from a different region
     */
    @Test
    public void listContentOnPublicBucketFromDifferentRegion() {
        S3StorageService storageService = new S3StorageService(
                "aind-msma-morphology-data",
                null,
                "us-west-2", // if any system credentials (~/.aws) are set the correct region must be used
                null,
                null
        );
        class TestData {
            final String contentLocation;
            final int depth;
            final int offset;
            final int expectedNodes;

            TestData(String contentLocation, int depth, int offset, int expectedNodes) {
                this.contentLocation = contentLocation;
                this.depth = depth;
                this.offset = offset;
                this.expectedNodes = expectedNodes;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("segmentation/exaSPIM_653159_zarr/", 0, 0, 8),
        };
        for (TestData td : testData) {
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
            try (InputStream nodeContentStream = storageService.readContent(n.getObjectKey())) {
                ByteStreams.copy(nodeContentStream, testDataStream);
            }
        }
        String testDataContent = testDataStream.toString();
        assertNotNull(testDataContent);
    }

}
