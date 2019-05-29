package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class DataContentUtils {
    public static DataNodeInfo createDataNodeInfo(Path rootPath,
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
}
