package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileListDataContent extends AbstractDataContent {

    private final List<DataNodeInfo> dataNodeList;
    private final Function<Path, InputStream> pathToStreamHandler;

    FileListDataContent(ContentFilterParams contentFilterParams, Path rootPath, Function<Path, InputStream> pathToStreamHandler, List<Path> fileList) {
        super(contentFilterParams);
        this.pathToStreamHandler = pathToStreamHandler;
        this.dataNodeList = fileList.stream()
                .map(p -> DataContentUtils.createDataNodeInfo(rootPath, p, Files.isDirectory(p), p.toFile().length()))
                .collect(Collectors.toList());
    }

    @Override
    public List<DataNodeInfo> listDataNodes() {
        return dataNodeList;
    }

    @Override
    public InputStream streamDataNode(DataNodeInfo dn) {
        if (dn.isCollectionFlag()) {
            throw new IllegalArgumentException("Streaming is not supported for directories - " + dn.getNodeAccessURL());
        } else {
            return pathToStreamHandler.apply(Paths.get(URI.create(dn.getNodeAccessURL())));
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("dataNodeList", dataNodeList)
                .toString();
    }
}
