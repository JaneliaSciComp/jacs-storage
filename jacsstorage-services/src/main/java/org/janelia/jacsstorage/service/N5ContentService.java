package org.janelia.jacsstorage.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.impl.n5.N5ReaderProvider;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for reading and writing content to a specified storage URI
 */
public class N5ContentService {

    private static final Logger LOG = LoggerFactory.getLogger(N5ContentService.class);

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class N5Node {
        private final String path;
        private final List<N5Node> children;
        private final N5Attributes metadata;

        private N5Node(String path, DatasetAttributes datasetAttributes) {
            this.path = path;
            this.children = new ArrayList<>();
            this.metadata = datasetAttributes != null ? new N5Attributes(datasetAttributes) : null;
        }

        public String getPath() {
            return path;
        }

        public List<N5Node> getChildren() {
            return children;
        }

        public N5Attributes getMetadata() {
            return metadata;
        }

        public boolean isDatasetFlag() {
            return metadata != null;
        }

        void addChild(N5Node node) {
            children.add(node);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("path", path)
                    .toString();
        }
    }

    public static class N5Attributes {
        private final DatasetAttributes datasetAttributes;

        N5Attributes(DatasetAttributes datasetAttributes) {
            this.datasetAttributes = datasetAttributes;
        }

        public int[] getBlockSize() {
            return datasetAttributes.getBlockSize();
        }

        public long[] getDimensions() {
            return datasetAttributes.getDimensions();
        }

        public String getDataType() {
            return datasetAttributes.getDataType().name();
        }

        public Map<String, String> getCompression() {
            return ImmutableMap.of("type", datasetAttributes.getCompression().getType());
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
        N5Node root;
        if (n5Reader.exists("/" + N5KeyValueReader.ATTRIBUTES_JSON)) {
            root = new N5Node("/", n5Reader.getDatasetAttributes("/"));
        } else {
            root = new N5Node("/", null);
        }
        discoverAndParseRecursive(n5Reader, root, 0, maxDepth);
        return root;
    }

    private void discoverAndParseRecursive(N5Reader n5Reader, N5Node root, int currentDepth, int maxDepth) {
        try {
            LOG.debug("Discover {}", root);
            String[] datasetPaths = n5Reader.list(root.getPath());
            updateChildren(n5Reader, root, datasetPaths, currentDepth, maxDepth);
            LOG.debug("Finished discover {}", root);
        } catch (ContentException e) {
            throw e;
        } catch (Exception e) {
            throw new ContentException("Error discovering " + root.getPath(), e);
        }
    }

    private void updateChildren(N5Reader n5Reader, N5Node parentNode, String[] nodePaths, int currentDepth, int maxDepth) {
        LOG.debug("Update children for {}", parentNode);
        for (String nodePath : nodePaths) {
            String fullNodePath = parentNode.getPath().equals("/") ? "/" + nodePath : parentNode.getPath() + "/" + nodePath;
            if (n5Reader.exists(fullNodePath + "/" + N5KeyValueReader.ATTRIBUTES_JSON)) {
                DatasetAttributes  datasetAttributes = n5Reader.getDatasetAttributes(fullNodePath);
                parentNode.addChild(new N5Node(fullNodePath, datasetAttributes));
            } else {
                parentNode.addChild(new N5Node(fullNodePath, null));
            }
        }
        LOG.debug("Finished updating children for {}", parentNode);
        if (currentDepth + 1 < maxDepth) {
            for (N5Node childNode : parentNode.getChildren()) {
                discoverAndParseRecursive(n5Reader, childNode, currentDepth + 1, maxDepth);
            }
        }
    }
}
