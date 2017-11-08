package org.janelia.jacsstorage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

class PingStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PingStorageAgentRequestHandler.class);

    PingStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest) {
        super(socketChannelStorageAgentRequest);
    }

    @Override
    public void handleAgentRequest() {
        socketChannelStorageAgentRequest.shutdownInput();
        socketChannelStorageAgentRequest.switchChannelToWriteMode();
    }

    @Override
    public void handleAgentResponse() {
        socketChannelStorageAgentRequest.writeResponse(new StorageMessageResponse(StorageMessageResponse.OK, "OK"));
        socketChannelStorageAgentRequest.close();
    }
}
