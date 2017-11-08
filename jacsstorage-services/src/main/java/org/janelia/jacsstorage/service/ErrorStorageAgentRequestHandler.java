package org.janelia.jacsstorage.service;

class ErrorStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
    private final String errrorMessage;

    ErrorStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest, String errrorMessage) {
        super(socketChannelStorageAgentRequest);
        this.errrorMessage = errrorMessage;
    }

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
