package org.janelia.jacsstorage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

class ErrorStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ErrorStorageAgentRequestHandler.class);

    private final String errrorMessage;

    ErrorStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest, String errrorMessage) {
        super(socketChannelStorageAgentRequest);
        this.errrorMessage = errrorMessage;
    }

    @LogStorageEvent(
            eventName = "ERROR_HANDLING_STORAGE_REQUEST"
    )
    @Override
    public void handleAgentRequest() {
        socketChannelStorageAgentRequest.shutdownInput();
        socketChannelStorageAgentRequest.switchChannelToWriteMode();
    }

    @Override
    public void handleAgentResponse() {
        socketChannelStorageAgentRequest.writeResponse(new StorageMessageResponse(StorageMessageResponse.ERROR, errrorMessage));
        socketChannelStorageAgentRequest.close();
    }
}
