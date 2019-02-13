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
import java.util.function.Function;
import java.util.function.Supplier;

public class SingleFileDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileDataContent.class);

    private final Path dataPath;
    private final Function<Path, InputStream> pathToStreamHandler;

    SingleFileDataContent(ContentFilterParams contentFilterParams, Path dataPath, Function<Path, InputStream> pathToStreamHandler) {
        super(contentFilterParams);
        this.dataPath = dataPath;
        this.pathToStreamHandler = pathToStreamHandler;
    }

    @Override
    public List<DataNodeInfo> listDataNodes() {
        return ImmutableList.of(DataContentUtils.createDataNodeInfo(dataPath, dataPath, Files.isDirectory(dataPath), dataPath.toFile().length()));
    }

    @Override
    public InputStream streamDataNode(DataNodeInfo dn) {
        if (dn.isCollectionFlag()) {
            throw new IllegalArgumentException("Streaming is not supported for directories - " + dn.getNodeAccessURL());
        } else if (!dn.getNodeAccessURL().equals(dataPath.toUri().toString())) {
            LOG.error("Invalid parameter: {} does not match the data path set {}", dn, dataPath);
            throw new IllegalArgumentException("Invalid data node info parameter: " + dn.getNodeAccessURL());
        } else {
            return pathToStreamHandler.apply(Paths.get(URI.create(dn.getNodeAccessURL())));
        }
    }
}
