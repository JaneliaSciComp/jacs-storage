package org.janelia.jacsstorage.clients.api.http;

import jakarta.ws.rs.client.Client;

public interface HttpClientProvider {
    Client getClient();
}
