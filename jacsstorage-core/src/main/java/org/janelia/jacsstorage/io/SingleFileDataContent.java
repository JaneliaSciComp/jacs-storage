package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SingleFileDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileDataContent.class);

    private final Path dataPath;

    public SingleFileDataContent(ContentFilterParams contentFilterParams, Path dataPath) {
        super(contentFilterParams);
        this.dataPath = dataPath;
    }

    @Override
    public List<DataNodeInfo> listDataNodes() {
        return ImmutableList.of(DataContentUtils.createDataNodeInfo(dataPath, dataPath, Files.isDirectory(dataPath), dataPath.toFile().length()));
    }

    @Override
    public InputStream streamDataNode(DataNodeInfo dn) {
        try {
            if (dn.isCollectionFlag()) {
                throw new IllegalArgumentException("Streaming is not supported for directories - " + dn.getNodeAccessURL());
            } else if (!dn.getNodeAccessURL().equals(dataPath.toUri().toString())) {
                LOG.error("Invalid parameter: {} does not match the data path set {}", dn, dataPath);
                throw new IllegalArgumentException("Invalid data node info parameter: " + dn.getNodeAccessURL());
            } else {
                return Files.newInputStream(Paths.get(URI.create(dn.getNodeAccessURL())));
            }
        } catch (IOException e) {
            LOG.warn("Error reading content from {}", dn.getNodeAccessURL(), e);
            throw new IllegalStateException(e);
        }
    }
}
