package org.janelia.jacsstorage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

class RetrieveStreamStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RetrieveStreamStorageAgentRequestHandler.class);
    private final DataTransferService agentStorageProxy;

    RetrieveStreamStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest, DataTransferService agentStorageProxy) {
        super(socketChannelStorageAgentRequest);
        this.agentStorageProxy = agentStorageProxy;
    }

    @Override
    public void handleAgentRequest() {
        socketChannelStorageAgentRequest.switchChannelToWriteMode();
    }

    @Override
    public void handleAgentResponse() {
        StorageMessageResponse response;
        switch (socketChannelStorageAgentRequest.transferState.getState()) {
            case READ_DATA_STARTED:
                return;
            case READ_DATA:
            case READ_DATA_COMPLETE:
                if (socketChannelStorageAgentRequest.channelOutputBuffer == null) {
                    // no response has been sent yet
                    ByteBuffer responseBuffer = ByteBuffer.allocate(SocketChannelStorageAgentRequest.BUFFER_SIZE);
                    int nbytes;
                    try {
                        nbytes = agentStorageProxy.readData(responseBuffer, socketChannelStorageAgentRequest.transferState);
                    } catch (IOException e) {
                        nbytes = -1;
                        LOG.error("Error reading stored data {}", socketChannelStorageAgentRequest.transferState, e);
                    }
                    if (nbytes == -1) {
                        // nothing read from the data channel
                        if (socketChannelStorageAgentRequest.transferState.getState() == State.READ_DATA_ERROR) {
                            response = new StorageMessageResponse(StorageMessageResponse.ERROR, socketChannelStorageAgentRequest.transferState.getErrorMessage());
                        } else {
                            response = new StorageMessageResponse(StorageMessageResponse.OK, socketChannelStorageAgentRequest.transferState.getErrorMessage());
                        }
                        socketChannelStorageAgentRequest.writeResponse(response);
                        socketChannelStorageAgentRequest.close();
                        return;
                    } else if (nbytes > 0) {
                        socketChannelStorageAgentRequest.channelOutputBuffer = responseBuffer;
                        socketChannelStorageAgentRequest.channelOutputBuffer.flip();
                        response = new StorageMessageResponse(StorageMessageResponse.OK, socketChannelStorageAgentRequest.transferState.getErrorMessage());
                        socketChannelStorageAgentRequest.writeResponse(response);
                    }
                }
                // the response header was already sent so continue with sending the data
                if (!socketChannelStorageAgentRequest.channelOutputBuffer.hasRemaining()) {
                    socketChannelStorageAgentRequest.channelOutputBuffer.clear();
                    // get more data to send
                    try {
                        if (agentStorageProxy.readData(socketChannelStorageAgentRequest.channelOutputBuffer, socketChannelStorageAgentRequest.transferState) == -1) {
                            // nothing more to send so simply close the channel since sending the data will not have a response suffix
                            socketChannelStorageAgentRequest.close();
                            return;
                        }
                    } catch (IOException e) {
                        LOG.error("Error reading stored data from {}", socketChannelStorageAgentRequest.transferState, e);
                    }
                    socketChannelStorageAgentRequest.channelOutputBuffer.flip();
                }
                if (socketChannelStorageAgentRequest.writeOutputBuffer() == -1) {
                    // error writing to the channel so simply close the channel
                    socketChannelStorageAgentRequest.close();
                }
                return;
            case READ_DATA_ERROR:
                if (socketChannelStorageAgentRequest.channelOutputBuffer == null) {
                    response = new StorageMessageResponse(StorageMessageResponse.ERROR, socketChannelStorageAgentRequest.transferState.getErrorMessage());
                    socketChannelStorageAgentRequest.writeResponse(response);
                } // else it already sent the response header so just close the channel
                socketChannelStorageAgentRequest.close();
                return;
            default:
                LOG.warn("Invalid state {} while persisting data from {}", socketChannelStorageAgentRequest.transferState.getState(), socketChannelStorageAgentRequest.getRemoteAddress());
                return;
        }
    }

    StorageAgentRequestHandler initRequestHandler() {
        try {
            agentStorageProxy.beginDataTransfer(socketChannelStorageAgentRequest.transferState);
            return this;
        } catch (Exception e) {
            LOG.error("Error starting the data transfer {} from {}", socketChannelStorageAgentRequest.transferState, socketChannelStorageAgentRequest.getRemoteAddress(),  e);
            return new ErrorStorageAgentRequestHandler(socketChannelStorageAgentRequest, "Error starting the data retrieval");
        }
    }
}
