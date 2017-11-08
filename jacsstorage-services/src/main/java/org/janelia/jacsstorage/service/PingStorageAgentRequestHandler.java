package org.janelia.jacsstorage.service;

class PingStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
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
