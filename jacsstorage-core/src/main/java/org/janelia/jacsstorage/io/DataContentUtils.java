package org.janelia.jacsstorage.io;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import org.apache.commons.compress.archivers.tar.TarConstants;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;

public class DataContentUtils {

    static DataNodeInfo createDataNodeInfo(Path rootPath,
                                           Path nodePath,
                                           boolean collectionFlag,
                                           long size) {
        DataNodeInfo ni = new DataNodeInfo();
        ni.setStorageRootPathURI(StoragePathURI.createAbsolutePathURI(rootPath.toString().replace(File.separatorChar, '/')));
        ni.setNodeAccessURL(nodePath.toUri().toString());
        ni.setNodeRelativePath(rootPath.relativize(nodePath).toString().replace(File.separatorChar, '/'));
        ni.setCollectionFlag(collectionFlag);
        ni.setSize(size);
        return ni;
    }

    public static Comparator<DataNodeInfo> getDataNodePathComparator() {
        return (dn1, dn2) -> {
            Path p1 = Paths.get(dn1.getNodeRelativePath());
            Path p2 = Paths.get(dn2.getNodeRelativePath());
            if (p1.getNameCount() < p2.getNameCount()) {
                return -1;
            } else if (p1.getNameCount() > p2.getNameCount()) {
                return 1;
            } else {
                if (dn1.isCollectionFlag() && !dn2.isCollectionFlag()) {
                    return -1;
                } else if (!dn1.isCollectionFlag() && dn2.isCollectionFlag()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
    }

    public static long calculateTarEntrySize(long physicalDataNodeSize) {
        long entrySize = physicalDataNodeSize + TarConstants.DEFAULT_RCDSIZE;
        if (entrySize % TarConstants.DEFAULT_RCDSIZE != 0) {
            // tar entry size should be an exact multiple of the record size
            return ((entrySize + TarConstants.DEFAULT_RCDSIZE) / TarConstants.DEFAULT_RCDSIZE) * TarConstants.DEFAULT_RCDSIZE;
        } else {
            return entrySize;
        }
    }
}
