package org.janelia.jacsstorage.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for reading and writing content to a specified storage URI
 */
public class N5ContentService {

    private static final Logger LOG = LoggerFactory.getLogger(N5ContentService.class);

    public static class N5Node {
        private final String path;
        private final List<N5Node> children;
        private DatasetAttributes metadata;

        private N5Node(String path) {
            this.path = path;
            this.children = new ArrayList<>();
        }

        public String getPath() {
            return path;
        }

        public List<N5Node> getChildren() {
            return children;
        }

        public DatasetAttributes getMetadata() {
            return metadata;
        }

        void setMetadata(DatasetAttributes metadata) {
            this.metadata = metadata;
        }

        public boolean isDatasetFlag() {
            return metadata != null;
        }

        void addChild(N5Node node) {
            children.add(node);
        }
    }

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
    public N5Node getN5Container(JADEStorageURI storageURI, int maxDepth) {
        N5Reader n5Reader = n5ReaderProvider.getN5Reader(storageURI);
        N5Node root = new N5Node("/");
        discoverAndParseRecursive(n5Reader, root, 0, maxDepth);
        return root;
    }

    private void discoverAndParseRecursive(N5Reader n5Reader, N5Node root, int currentDepth, int maxDepth) {
        try {
            String[] datasetPaths = n5Reader.list(root.getPath());
            updateChildren(n5Reader, root, datasetPaths, currentDepth, maxDepth);
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    private void updateChildren(N5Reader n5Reader, N5Node parentNode, String[] nodePaths, int currentDepth, int maxDepth) {
        Arrays.sort(nodePaths);

        for (String nodePath : nodePaths) {
            String fullNodePath = parentNode.getPath().equals("/") ? "/" + nodePath : parentNode.getPath() + "/" + nodePath;
            N5Node node = new N5Node(fullNodePath);
            if (n5Reader.datasetExists(fullNodePath)) {
                node.setMetadata(n5Reader.getDatasetAttributes(fullNodePath));
            }
            parentNode.addChild(node);
            if (currentDepth + 1 < maxDepth) {
                discoverAndParseRecursive(n5Reader, node, currentDepth + 1, maxDepth);
            }
        }
    }
}
