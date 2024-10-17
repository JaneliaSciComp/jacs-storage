package org.janelia.jacsstorage.clients;

import java.nio.file.Paths;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

public class N5Node {
    private final String path;
    private final List<N5Node> children;
    private final DatasetAttributes metadata;

    @JsonCreator
    public N5Node(String path, List<N5Node> children, DatasetAttributes metadata) {
        this.path = path;
        this.children = children;
        this.metadata = metadata;
    }

    public String getNodeName() {
        return Paths.get(path).getFileName().toString();
    }

    public String getPath() {
        return path;
    }

    public List<N5Node> getChildren() {
        return children;
    }

    public DatasetAttributes getMetadata() {
        return metadata;
    }
}
