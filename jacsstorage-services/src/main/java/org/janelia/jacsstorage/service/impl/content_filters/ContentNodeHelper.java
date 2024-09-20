package org.janelia.jacsstorage.service.impl.content_filters;

import java.util.List;

import org.janelia.jacsstorage.service.ContentNode;

public class ContentNodeHelper {
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
