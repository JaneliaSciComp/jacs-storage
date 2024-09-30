package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.jacsstorage.service.impl.n5.N5ViewerMultichannelMetadata;
import org.janelia.saalfeldlab.n5.N5DatasetDiscoverer;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5TreeNode;
import org.janelia.saalfeldlab.n5.metadata.N5CosemMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5CosemMultiScaleMetadata;
import org.janelia.saalfeldlab.n5.metadata.N5GenericSingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5MetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5SingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5ViewerMultiscaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.canonical.CanonicalMetadataParser;

/**
 * Service for reading and writing content to a specified storage URI
 */
public class N5ContentService {

    public static final N5MetadataParser<?>[] N5_GROUP_PARSERS = new N5MetadataParser[]{
            new N5CosemMultiScaleMetadata.CosemMultiScaleParser(),
            new N5ViewerMultiscaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5ViewerMultichannelMetadata.N5ViewerMultichannelMetadataParser()
    };

    public static final N5MetadataParser<?>[] N5_METADA_PARSERS = new N5MetadataParser[] {
            new N5CosemMetadataParser(),
            new N5SingleScaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5GenericSingleScaleMetadataParser()
    };

    private final N5ReaderProvider n5ReaderProvider;

    @Inject
    N5ContentService(N5ReaderProvider n5ReaderProvider) {
        this.n5ReaderProvider = n5ReaderProvider;
    }

    /**
     * Check if the content identified by the specified URI exists.
     *
     * @param storageURI
     * @return
     */
    public N5TreeNode getN5Container(JADEStorageURI storageURI) {
        N5Reader n5Reader = n5ReaderProvider.getN5Reader(storageURI);
        N5DatasetDiscoverer datasetDiscoverer = new N5DatasetDiscoverer(
                n5Reader,
                Executors.newCachedThreadPool(),
                Arrays.asList(N5_METADA_PARSERS),
                Arrays.asList(N5_GROUP_PARSERS));
        try {
            return datasetDiscoverer.discoverAndParseRecursive("/");
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }

}
