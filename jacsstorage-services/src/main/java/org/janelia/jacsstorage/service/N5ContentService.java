package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import com.google.common.collect.Streams;
import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.jacsstorage.service.impl.n5.N5ViewerMultichannelMetadata;
import org.janelia.saalfeldlab.n5.N5DatasetDiscoverer;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5TreeNode;
import org.janelia.saalfeldlab.n5.metadata.N5CosemMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5CosemMultiScaleMetadata;
import org.janelia.saalfeldlab.n5.metadata.N5GenericSingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5Metadata;
import org.janelia.saalfeldlab.n5.metadata.N5MetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5MultiScaleMetadata;
import org.janelia.saalfeldlab.n5.metadata.N5SingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.N5ViewerMultiscaleMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.canonical.CanonicalMetadataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.janelia.saalfeldlab.n5.N5DatasetDiscoverer.parseMetadata;

/**
 * Service for reading and writing content to a specified storage URI
 */
public class N5ContentService {

    private static final Logger LOG = LoggerFactory.getLogger(N5ContentService.class);

    public static final N5MetadataParser<?>[] N5_GROUP_PARSERS = new N5MetadataParser[]{
            new N5CosemMultiScaleMetadata.CosemMultiScaleParser(),
            new N5ViewerMultiscaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5ViewerMultichannelMetadata.N5ViewerMultichannelMetadataParser()
    };

    public static final N5MetadataParser<?>[] N5_METADATA_PARSERS = new N5MetadataParser[]{
            new N5CosemMetadataParser(),
            new N5SingleScaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5GenericSingleScaleMetadataParser(),
            new N5MultiScaleMetadata.MultiScaleParser()
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
     * @param storageURI
     * @return
     */
    public N5TreeNode getN5Container(JADEStorageURI storageURI) {
        N5Reader n5Reader = n5ReaderProvider.getN5Reader(storageURI);
        discoverAndParseRecursive(n5Reader, "/");
        N5DatasetDiscoverer datasetDiscoverer = new N5DatasetDiscoverer(
                n5Reader,
                executorService,
                Arrays.asList(N5_METADATA_PARSERS),
                Arrays.asList(N5_GROUP_PARSERS));
        try {
            return datasetDiscoverer.discoverAndParseRecursive("/");
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }

    private void discoverAndParseRecursive(N5Reader n5Reader, String groupPath) {
        String[] children = n5Reader.list(groupPath);

        for (String child : children) {
            String childPath = groupPath.equals("/") ? "/" + child : groupPath + "/" + child;

            // Check if the path is a dataset
            if (n5Reader.datasetExists(childPath)) {
                System.out.println("Dataset: " + childPath);

                // Attempt to parse metadata using available parsers
                N5Metadata metadata = tryAllParsers(n5Reader, childPath);
                if (metadata != null) {
                    System.out.println("Metadata: " + metadata);
                } else {
                    LOG.info("Metadata from {} could not be parsed with the currently registered parsers", childPath);
                }
            } else {
                System.out.println("Group: " + childPath);
                // Recursively explore the child group
                discoverAndParseRecursive(n5Reader, childPath);
            }
        }
    }

    private static N5Metadata tryAllParsers(N5Reader n5Reader, String datasetPath) {
        return parseMetadata(n5Reader, datasetPath, N5_METADATA_PARSERS)
                .map(md -> (N5Metadata) md)
                .orElseGet(() -> parseMetadata(n5Reader, datasetPath, N5_GROUP_PARSERS).orElse(null));
    }

    private static Optional<? extends N5Metadata> parseMetadata(N5Reader n5Reader, String datasetPath, N5MetadataParser<? extends N5Metadata>[] parsers) {
        for (N5MetadataParser<? extends N5Metadata> parser : parsers) {
            try {
                Optional<? extends N5Metadata> metadata = parser.parseMetadata(n5Reader, datasetPath);
                if (metadata.isPresent()) {
                    LOG.info("Successfully parsed {} with {}", datasetPath, parser.getClass().getSimpleName());
                    return metadata;
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse {} with {}: {}", datasetPath, parser.getClass().getSimpleName(), e.getMessage());
            }
        }
        return Optional.empty();
    }
}
