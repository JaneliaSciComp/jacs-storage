package org.janelia.jacsstorage.service;

import java.util.concurrent.Executors;

import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;
import org.janelia.saalfeldlab.n5.universe.N5TreeNode;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class N5ContentServiceTest {

    private static S3AdapterProvider s3AdapterProvider;

    @BeforeClass
    public static void setUp() {
        s3AdapterProvider = new S3AdapterProvider();
    }

    @Test
    public void readN5TreeFromS3() {
        N5ContentService testService = new N5ContentService(
                new N5ReaderProvider(s3AdapterProvider, "us-east-1", true, 512, 128),
                Executors.newSingleThreadExecutor()
        );
        // this test is a bit flaky because I don't control this location so it may disappear
        N5TreeNode node = testService.getN5Container(JADEStorageURI.createStoragePathURI(
                "s3://janelia-bigstitcher-spark/IP/dataset.n5/setup0/timepoint18",
                JADEOptions.create()));
        assertNotNull(node);
        assertFalse(node.childrenList().isEmpty());
    }
}
