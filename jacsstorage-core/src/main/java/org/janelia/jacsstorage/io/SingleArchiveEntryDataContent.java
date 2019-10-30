package org.janelia.jacsstorage.io;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleArchiveEntryDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(SingleArchiveEntryDataContent.class);

    private final String entryName;
    private final DataNodeInfo dataNode;
    private final InputStream contentStream;

    SingleArchiveEntryDataContent(ContentFilterParams contentFilterParams, String entryName, long size, InputStream contentStream) {
        super(contentFilterParams);
        this.entryName = entryName;
        this.contentStream = contentStream;
        this.dataNode = DataContentUtils.createDataNodeInfo(getEntryNameAsPath(), getEntryNameAsPath(), false, size);
    }

    @Override
    public Stream<DataNodeInfo> streamDataNodes() {
        return Stream.of(dataNode);
    }

    @Override
    public InputStream streamDataNode(DataNodeInfo dn) {
        if (dn.isCollectionFlag()) {
            throw new IllegalArgumentException("Streaming is not supported for directories - " + dn.getNodeAccessURL());
        } else if (!dn.getNodeAccessURL().equals(getEntryNameAsPath().toUri().toString())) {
            LOG.error("Invalid parameter: {} does not match the data path set {}", dn, entryName);
            throw new IllegalArgumentException("Invalid data node info parameter: " + dn.getNodeAccessURL());
        } else {
            return contentStream;
        }
    }

    private Path getEntryNameAsPath() {
        return Paths.get(entryName);
    }
}
