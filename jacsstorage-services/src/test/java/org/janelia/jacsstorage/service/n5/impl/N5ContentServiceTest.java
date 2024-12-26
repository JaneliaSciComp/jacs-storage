package org.janelia.jacsstorage.service.n5.impl;

import java.util.concurrent.Executors;

import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.n5.N5ContentService;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;
import org.janelia.jacsstorage.service.s3.impl.S3AdapterProviderImpl;
import org.janelia.saalfeldlab.n5.universe.N5TreeNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class N5ContentServiceTest {

    private static S3AdapterProvider s3AdapterProvider;

    @BeforeAll
    public static void setUp() {
        s3AdapterProvider = new S3AdapterProviderImpl();
    }

    @Test
    public void readN5TreeFromS3() {
        N5ContentService testService = new N5ContentServiceImpl(
                new N5ReaderProviderImpl(s3AdapterProvider, "us-east-1"),
                Executors.newSingleThreadExecutor()
        );
        N5TreeNode node = testService.getN5Container(JADEStorageURI.createStoragePathURI(
                "s3://janelia-bigstitcher-spark/Stitching/dataset.n5/setup0/timepoint0",
                JADEOptions.create()));
        assertNotNull(node);
        assertFalse(node.childrenList().isEmpty());
    }
}
