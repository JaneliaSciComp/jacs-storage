package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import jakarta.inject.Inject;

import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.jacsstorage.service.impl.n5.N5ViewerMultichannelMetadata;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.universe.N5DatasetDiscoverer;
import org.janelia.saalfeldlab.n5.universe.N5TreeNode;
import org.janelia.saalfeldlab.n5.universe.metadata.N5CosemMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5CosemMultiScaleMetadata;
import org.janelia.saalfeldlab.n5.universe.metadata.N5GenericSingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5Metadata;
import org.janelia.saalfeldlab.n5.universe.metadata.N5MetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5MultiScaleMetadata;
import org.janelia.saalfeldlab.n5.universe.metadata.N5SingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5ViewerMultiscaleMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.canonical.CanonicalMetadataParser;

/**
 * Service for reading and writing content to a specified storage URI
 */
public class N5ContentService {

    private static final N5MetadataParser<? extends N5Metadata>[] N5_GROUP_PARSERS = new N5MetadataParser<?>[]{
            new N5CosemMultiScaleMetadata.CosemMultiScaleParser(),
            new N5ViewerMultiscaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5ViewerMultichannelMetadata.N5ViewerMultichannelMetadataParser()
    };

    private static final N5MetadataParser<? extends N5Metadata>[] N5_METADATA_PARSERS = new N5MetadataParser<?>[]{
            new N5CosemMetadataParser(),
            new N5SingleScaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5MultiScaleMetadata.MultiScaleParser(),
            new N5GenericSingleScaleMetadataParser()
    };

    private final N5ReaderProvider n5ReaderProvider;
    private final ExecutorService executorService;

    @Inject
    N5ContentService(N5ReaderProvider n5ReaderProvider, @PooledResource ExecutorService executorService) {
        this.n5ReaderProvider = n5ReaderProvider;
        this.executorService = executorService;
    }

    /**
     * Check if the content identified by the specified URI exists.
     *
     * @param storageURI N5 container location
     * @return N5TreeNode
     */
    public N5TreeNode getN5Container(JADEStorageURI storageURI) {
        try {
            N5Reader n5Reader = n5ReaderProvider.getN5Reader(storageURI);
            N5DatasetDiscoverer datasetDiscoverer = new N5DatasetDiscoverer(
                    n5Reader,
                    executorService,
                    Arrays.asList(N5_METADATA_PARSERS),
                    Arrays.asList(N5_GROUP_PARSERS));
            return datasetDiscoverer.discoverAndParseRecursive("/");
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }
}
