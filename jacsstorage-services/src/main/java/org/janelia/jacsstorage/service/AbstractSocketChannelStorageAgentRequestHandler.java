package org.janelia.jacsstorage.service;

abstract class AbstractSocketChannelStorageAgentRequestHandler implements StorageAgentRequestHandler {
    protected final SocketChannelStorageAgentRequest socketChannelStorageAgentRequest;

    public AbstractSocketChannelStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest) {
        this.socketChannelStorageAgentRequest = socketChannelStorageAgentRequest;
    }

}
