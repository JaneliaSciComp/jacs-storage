package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;

import javax.activation.MimetypesFileTypeMap;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.function.BiFunction;

abstract class AbstractBundleReader implements BundleReader {

    final ContentHandlerProvider contentHandlerProvider;
    private final MimetypesFileTypeMap mimetypesFileTypeMap;

    AbstractBundleReader(ContentHandlerProvider contentHandlerProvider) {
        this.contentHandlerProvider = contentHandlerProvider;
        this.mimetypesFileTypeMap = new MimetypesFileTypeMap();
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long readBundle(String source, ContentFilterParams filterParams, OutputStream stream) {
        return readDataEntry(source, "", filterParams, stream);
    }

    DataNodeInfo pathToDataNodeInfo(Path rootPath, Path nodePath, BiFunction<Path, Path, String> nodePathMapper) {
        try {
            DataNodeInfo dataNodeInfo = new DataNodeInfo();
            BasicFileAttributes attrs = Files.readAttributes(nodePath, BasicFileAttributes.class);
            dataNodeInfo.setNodeRelativePath(nodePathMapper.apply(rootPath, nodePath));
            dataNodeInfo.setSize(attrs.size());
            dataNodeInfo.setMimeType(getMimeType(nodePath));
            dataNodeInfo.setCollectionFlag(attrs.isDirectory());
            dataNodeInfo.setCreationTime(new Date(attrs.creationTime().toMillis()));
            dataNodeInfo.setLastModified(new Date(attrs.lastModifiedTime().toMillis()));
            return dataNodeInfo;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    String getMimeType(String nodePath) {
        return getMimeType(Paths.get(nodePath));
    }

    String getMimeType(Path nodePath) {
        return mimetypesFileTypeMap.getContentType(nodePath.toFile());
    }
}
