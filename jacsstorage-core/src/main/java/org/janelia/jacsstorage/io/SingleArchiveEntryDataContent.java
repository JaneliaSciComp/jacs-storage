package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SingleArchiveEntryDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(SingleArchiveEntryDataContent.class);

    private final String entryName;
    private final List<DataNodeInfo> dataNodeList;
    private final InputStream contentStream;

    SingleArchiveEntryDataContent(ContentFilterParams contentFilterParams, String entryName, long size, InputStream contentStream) {
        super(contentFilterParams);
        this.entryName = entryName;
        this.contentStream = contentStream;
        this.dataNodeList = ImmutableList.of(DataContentUtils.createDataNodeInfo(getEntryNameAsPath(), getEntryNameAsPath(), false, size));
    }

    @Override
    public List<DataNodeInfo> listDataNodes() {
        return dataNodeList;
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
