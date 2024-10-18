package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class N5ContentServiceTest {

    @Test
    public void readN5TreeFromS3() {
        N5ContentService testService = new N5ContentService(
                new N5ReaderProvider("us-east-1")
        );
        N5ContentService.N5Node node = testService.getN5Container(JADEStorageURI.createStoragePathURI(
                "s3://janelia-bigstitcher-spark/Stitching/dataset.n5/setup0/timepoint0",
                new JADEStorageOptions()),
                2);
        assertNotNull(node);
        assertFalse(node.getChildren().isEmpty());
    }
}
