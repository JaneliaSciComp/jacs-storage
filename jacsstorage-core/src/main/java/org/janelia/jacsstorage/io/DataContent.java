package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

public interface DataContent {
    ContentFilterParams getContentFilterParams();
    Stream<DataNodeInfo> streamDataNodes();
    InputStream streamDataNode(DataNodeInfo dn);
}
