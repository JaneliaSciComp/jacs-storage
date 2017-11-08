package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;

class PersistStreamStorageAgentRequestHandler extends AbstractSocketChannelStorageAgentRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PersistStreamStorageAgentRequestHandler.class);

    private final DataTransferService agentStorageProxy;
    private final StorageAllocatorService storageAllocatorService;

    PersistStreamStorageAgentRequestHandler(SocketChannelStorageAgentRequest socketChannelStorageAgentRequest, DataTransferService agentStorageProxy, StorageAllocatorService storageAllocatorService) {
        super(socketChannelStorageAgentRequest);
        this.agentStorageProxy = agentStorageProxy;
        this.storageAllocatorService = storageAllocatorService;
    }

    @Override
    public void handleAgentRequest() {
        if (socketChannelStorageAgentRequest.lastTransferredBufferSize == -1 || socketChannelStorageAgentRequest.transferState.getState() == State.WRITE_DATA_ERROR) {
            // nothing left to read or error encountered while persisting the data
            socketChannelStorageAgentRequest.switchChannelToWriteMode();
            try {
                agentStorageProxy.endDataTransfer(socketChannelStorageAgentRequest.transferState);
            } catch (IOException e) {
                LOG.error("Error while completing data transfer from {}", socketChannelStorageAgentRequest.getRemoteAddress(), e);
            } finally {
                socketChannelStorageAgentRequest.shutdownInput();
            }
        } else {
            // persist the last read buffer
            int numWritten;
            try {
                numWritten = agentStorageProxy.writeData(socketChannelStorageAgentRequest.channelInputBuffer, socketChannelStorageAgentRequest.transferState);
            } catch (Exception e) {
                LOG.error("Error while persisting data retrieved from {}", socketChannelStorageAgentRequest.getRemoteAddress(), e);
                numWritten = -1;
            }
            if (numWritten == -1) {
                // error persisting the data
                socketChannelStorageAgentRequest.switchChannelToWriteMode();
                socketChannelStorageAgentRequest.shutdownInput();
            } else {
                socketChannelStorageAgentRequest.incrementNTransferredBytes(numWritten);
            }
        }
    }


    @Override
    public void handleAgentResponse() {
        StorageMessageResponse response;
        switch (socketChannelStorageAgentRequest.transferState.getState()) {
            case WRITE_DATA_STARTED:
            case WRITE_DATA:
                // still writing the data to the file system
                return;
            case WRITE_DATA_COMPLETE:
                // update the storage for the persisted data bundle
                storageAllocatorService.updateStorage(
                        socketChannelStorageAgentRequest.jacsCredentials,
                        new JacsBundleBuilder()
                                .dataBundleId(socketChannelStorageAgentRequest.transferState.getMessageType().getDataBundleId())
                                .usedSpaceInBytes(socketChannelStorageAgentRequest.transferState.getPersistedBytes())
                                .checksum(Base64.getEncoder().encodeToString(socketChannelStorageAgentRequest.transferState.getChecksum()))
                                .build());
                // and continue sending the response
            case WRITE_DATA_ERROR:
                // write the response
                response = new StorageMessageResponse(socketChannelStorageAgentRequest.transferState.getState() == State.WRITE_DATA_COMPLETE ? StorageMessageResponse.OK : StorageMessageResponse.ERROR,
                        socketChannelStorageAgentRequest.transferState.getErrorMessage());
                socketChannelStorageAgentRequest.writeResponse(response);
                socketChannelStorageAgentRequest.close();
                return;
            default:
                LOG.warn("Invalid state {} while persisting data from {}", socketChannelStorageAgentRequest.transferState.getState(), socketChannelStorageAgentRequest.getRemoteAddress());
                response = new StorageMessageResponse(StorageMessageResponse.ERROR,
                        "Invalid state " + socketChannelStorageAgentRequest.transferState.getState() + "while persisting data from " + socketChannelStorageAgentRequest.getRemoteAddress());
                socketChannelStorageAgentRequest.writeResponse(response);
                socketChannelStorageAgentRequest.close();
        }
    }

    @LogStorageEvent(
            eventName = "TCP_STREAM_STORAGE_DATA"
    )
    StorageAgentRequestHandler initRequestHandler() {
        try {
            agentStorageProxy.beginDataTransfer(socketChannelStorageAgentRequest.transferState);
            return this;
        } catch (Exception e) {
            LOG.error("Error starting the data transfer {} from {}", socketChannelStorageAgentRequest.transferState, socketChannelStorageAgentRequest.getRemoteAddress(),  e);
            return new ErrorStorageAgentRequestHandler(socketChannelStorageAgentRequest, "Error starting the data persistence");
        }
    }
}
