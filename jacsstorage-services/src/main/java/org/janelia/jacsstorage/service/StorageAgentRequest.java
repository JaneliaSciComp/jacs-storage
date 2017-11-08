package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.util.Optional;

interface StorageAgentRequest {
    int read();
    void close();
    Optional<StorageAgentRequestHandler> getRequestHandler();
}
