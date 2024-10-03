package org.janelia.jacsstorage.io;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;

public class FileListDataContent extends AbstractDataContent {

    private final Path rootPath;
    private final Supplier<Stream<DataNodeInfo>> dataNodeStreamProvider;
    private final Function<Path, InputStream> pathToStreamHandler;

    FileListDataContent(ContentAccessParams contentAccessParams,
                        Path rootPath,
                        Function<Path, InputStream> pathToStreamHandler,
                        Supplier<Stream<Path>> fileListProvider) {
        super(contentAccessParams);
        this.rootPath = rootPath;
        this.pathToStreamHandler = pathToStreamHandler;
        this.dataNodeStreamProvider = () -> fileListProvider.get()
                .map(p -> DataContentUtils.createDataNodeInfo(rootPath, p, Files.isDirectory(p), p.toFile().length()));
    }

    @Override
    public Stream<DataNodeInfo> streamDataNodes() {
        return dataNodeStreamProvider.get();
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
                .append("rootPath", rootPath)
                .toString();
    }
}
