package org.janelia.jacsstorage.clients.api.http;

import javax.ws.rs.client.Client;

public interface HttpClientProvider {
    Client getClient();
}
