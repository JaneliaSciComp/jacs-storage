package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;

import javax.activation.MimetypesFileTypeMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.function.BiFunction;

public abstract class AbstractBundleReader implements BundleReader {

    DataNodeInfo pathToDataNodeInfo(Path rootPath, Path nodePath, BiFunction<Path, Path, String> nodePathMapper) {
        try {
            DataNodeInfo dataNodeInfo = new DataNodeInfo();
            BasicFileAttributes attrs = Files.readAttributes(nodePath, BasicFileAttributes.class);
            dataNodeInfo.setNodeRelativePath(nodePathMapper.apply(rootPath, nodePath));
            dataNodeInfo.setSize(attrs.size());
            dataNodeInfo.setMimeType(new MimetypesFileTypeMap().getContentType(nodePath.toFile()));
            dataNodeInfo.setCollectionFlag(attrs.isDirectory());
            dataNodeInfo.setCreationTime(new Date(attrs.creationTime().toMillis()));
            dataNodeInfo.setLastModified(new Date(attrs.lastModifiedTime().toMillis()));
            return dataNodeInfo;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
