package org.janelia.jacsstorage.io;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleFileDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileDataContent.class);

    private final Path dataPath;
    private final Function<Path, InputStream> pathToStreamHandler;

    SingleFileDataContent(ContentAccessParams contentAccessParams, Path dataPath, Function<Path, InputStream> pathToStreamHandler) {
        super(contentAccessParams);
        this.dataPath = dataPath;
        this.pathToStreamHandler = pathToStreamHandler;
    }

    @Override
    public Stream<DataNodeInfo> streamDataNodes() {
        return Stream.of(DataContentUtils.createDataNodeInfo(dataPath, dataPath, Files.isDirectory(dataPath), dataPath.toFile().length()));
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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("dataPath", dataPath)
                .toString();
    }
}
