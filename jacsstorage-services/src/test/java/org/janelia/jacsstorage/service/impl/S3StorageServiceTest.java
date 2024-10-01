package org.janelia.jacsstorage.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class S3StorageServiceTest {

    @Test
    public void retrieveSingleFileContentFromS3Endpoint() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "https://s3.us-east-1.lyvecloud.seagate.com",
                "scicompsoft-public",
                "NNQ20KNJ2YCWWMPE",
                "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM"
        );
        List<ContentNode> contentNodes = storageService.listContentNodes("/scicompsoft/flynp/pipeline_info/software_versions.yml", new ContentFilterParams());
        assertEquals(1, contentNodes.size());
        String nodeContent = new String(ByteStreams.toByteArray(contentNodes.get(0).getContent()));
        assertTrue(nodeContent.length() > 0);
    }

    @Test
    public void listFolderContentFromS3Endpoint() throws IOException {
        S3StorageService storageService = new S3StorageService(
                "https://s3.us-east-1.lyvecloud.seagate.com",
                "scicompsoft-public",
                "NNQ20KNJ2YCWWMPE",
                "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM"
        );
        List<ContentNode> contentNodes = storageService.listContentNodes("scicompsoft/flynp/pipeline_info", new ContentFilterParams());
        assertTrue(contentNodes.size() > 0);
    }

    @Test
    public void retrieveSingleFileFromS3() throws IOException {
        S3StorageService storageService = new S3StorageService("janelia-neuronbridge-data-dev");
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentFilterParams()
                        .setEntryNamePattern("config.json")
                        .setMaxDepth(1));
        assertTrue(nodes.size() == 1);
        String nodeContent = new String(ByteStreams.toByteArray(nodes.get(0).getContent()));
        assertNotNull(nodeContent);
    }

    @Test
    public void retrieveSelectedFilesFromS3() throws IOException {
        S3StorageService storageService = new S3StorageService("janelia-neuronbridge-data-dev");
        List<ContentNode> nodes = storageService.listContentNodes("v3_3_0",
                new ContentFilterParams()
                        .addSelectedEntry("config.json")
                        .addSelectedEntry("DATA_NOTES.md")
                        .setMaxDepth(1));
        assertEquals(2, nodes.size());
    }

    @Test
    public void writeAndDeleteContentOnS3() throws IOException {
        S3StorageService storageService = new S3StorageService("janelia-neuronbridge-data-dev");
        String testContent = "This is some test content";
        long l = storageService.writeContent("myTest.txt", new ByteArrayInputStream(testContent.getBytes()));
        assertTrue(l == testContent.length());
        List<ContentNode> nodesAfterWrite = storageService.listContentNodes("myTest.txt", new ContentFilterParams());
        assertTrue(nodesAfterWrite.size() == 1);
        String nodeContent = new String(ByteStreams.toByteArray(nodesAfterWrite.get(0).getContent()));
        assertEquals(testContent, nodeContent);
        storageService.deleteContent("myTest.txt");
        List<ContentNode> nodesAfterDelete = storageService.listContentNodes("myTest.txt", new ContentFilterParams());
        assertTrue(nodesAfterDelete.size() == 0);
    }

    @Test
    public void retrievePrefixFromS3() throws IOException {
        S3StorageService storageService = new S3StorageService("janelia-neuronbridge-data-dev");
        ByteArrayOutputStream testDataStream = new ByteArrayOutputStream();
        List<ContentNode> contentNodes = storageService.listContentNodes("v3_3_0/schemas", new ContentFilterParams());
        for (ContentNode n : contentNodes) {
            ByteStreams.copy(n.getContent(), testDataStream);
        }
        String testDataContent = testDataStream.toString();
        assertNotNull(testDataContent);
    }

}
