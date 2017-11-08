package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.util.Optional;

interface StorageAgentRequest {
    int read() throws IOException;
    void close();
    Optional<StorageAgentRequestHandler> getRequestHandler();
}
