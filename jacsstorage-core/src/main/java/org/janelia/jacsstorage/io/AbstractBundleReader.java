package org.janelia.jacsstorage.io;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;

import javax.activation.MimetypesFileTypeMap;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.function.BiFunction;

public abstract class AbstractBundleReader implements BundleReader {

    @TimedMethod(
            argList = {0},
            logResult = true
    )
    @Override
    public TransferInfo readBundle(String source, OutputStream stream) {
        try {
//            HashingOutputStream hashingOutputStream = new HashingOutputStream(Hashing.sha256(), stream);
            long nBytes = readBundleBytes(source, stream);
            return new TransferInfo(nBytes, new byte[0]);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected abstract long readBundleBytes(String source, OutputStream stream) throws Exception;

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
