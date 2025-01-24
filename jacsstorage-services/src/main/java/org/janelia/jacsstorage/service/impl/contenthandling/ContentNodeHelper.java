package org.janelia.jacsstorage.service.impl.contenthandling;

import java.util.List;

import org.apache.commons.compress.archivers.tar.TarConstants;
import org.janelia.jacsstorage.service.ContentNode;

public class ContentNodeHelper {
    public static long calculateTarEntrySize(long physicalNodeSize) {
        long entrySize = physicalNodeSize + TarConstants.DEFAULT_RCDSIZE;
        if (entrySize % TarConstants.DEFAULT_RCDSIZE != 0) {
            // tar entry size should be an exact multiple of the record size
            return ((entrySize + TarConstants.DEFAULT_RCDSIZE) / TarConstants.DEFAULT_RCDSIZE) * TarConstants.DEFAULT_RCDSIZE;
        } else {
            return entrySize;
        }
    }

    public static String commonPrefix(List<ContentNode> contentNodes) {
        StringBuilder commonPrefix = new StringBuilder();
        String[][] prefixes = contentNodes.stream()
                .map(n -> n.getPrefix().split("/"))
                .toArray(String[][]::new);

        for (int j = 0; j < prefixes[0].length; j++) {
            String s = prefixes[0][j];
            for (int i = 1; i < contentNodes.size(); i++) {
                if (!s.equals(prefixes[i][j]))
                    return commonPrefix.toString();
            }
            if (commonPrefix.length() > 0) {
                commonPrefix.append('/');
            }
            commonPrefix.append(s);
        }
        return commonPrefix.toString();
    }
}
