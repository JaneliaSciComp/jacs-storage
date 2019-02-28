package org.janelia.jacsstorage.io;

import java.io.OutputStream;

public interface ContentConverter {
    boolean support(String filterType);
    long convertContent(DataContent dataContent, OutputStream outputStream);
}
