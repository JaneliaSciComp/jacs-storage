package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;

import java.io.InputStream;
import java.util.List;

public interface DataContent {
    ContentFilterParams getContentFilterParams();
    List<DataNodeInfo> listDataNodes();
    InputStream streamDataNode(DataNodeInfo dn);
}
