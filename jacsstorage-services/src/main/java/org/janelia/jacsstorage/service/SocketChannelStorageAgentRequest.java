package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.security.JwtTokenCredentialsValidator;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Optional;

class SocketChannelStorageAgentRequest implements StorageAgentRequest {
    static final int BUFFER_SIZE = 2048;
    private static final Logger LOG = LoggerFactory.getLogger(SocketChannelStorageAgentRequest.class);

    private SocketChannel socketChannel;
    private final DataTransferService agentStorageProxy;
    private final StorageAllocatorService storageAllocatorService;
    private final JwtTokenCredentialsValidator authTokenValidator;
    private final StorageEventLogger storageEventLogger;

    final ByteBuffer channelInputBuffer;
    final TransferState<StorageMessageHeader> transferState;
    private SelectionKey selectionKey;
    private StorageAgentRequestHandler requestHandler;
    JacsCredentials jacsCredentials;
    ByteBuffer channelOutputBuffer;
    private long nTransferredBytes;
    volatile int lastTransferredBufferSize;

    SocketChannelStorageAgentRequest(SocketChannel socketChannel,
                                     DataTransferService agentStorageProxy,
                                     StorageAllocatorService storageAllocatorService,
                                     JwtTokenCredentialsValidator authTokenValidator,
                                     StorageEventLogger storageEventLogger) {
        this.socketChannel = socketChannel;
        this.agentStorageProxy = agentStorageProxy;
        this.storageAllocatorService = storageAllocatorService;
        this.authTokenValidator = authTokenValidator;
        this.storageEventLogger = storageEventLogger;
        channelInputBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        channelInputBuffer.limit(0); // empty
        this.transferState = new TransferState<>();
        nTransferredBytes = 0;
        lastTransferredBufferSize = 0;
    }

    @Override
    public int read() {
        lastTransferredBufferSize = 0;
        if (!channelInputBuffer.hasRemaining()) {
            channelInputBuffer.clear();
            try {
                lastTransferredBufferSize = socketChannel.read(channelInputBuffer);
            } catch (IOException e) {
                LOG.error("Error reading the input channel from {}", getRemoteAddress(), e);
                lastTransferredBufferSize = -1;
            } finally {
                channelInputBuffer.flip();
            }
        }
        return lastTransferredBufferSize;
    }

    @Override
    public void close() {
        try {
            if (socketChannel != null) {
                socketChannel.close();
            }
        } catch (IOException e) {
            LOG.warn("Error closing channel from {}", getRemoteAddress(), e);
        }
    }

    @Override
    public Optional<StorageAgentRequestHandler> getRequestHandler() {
        if (requestHandler == null) {
            try {
                if (lastTransferredBufferSize == -1) {
                    LOG.error("Input stream closed before reading the request");
                    requestHandler = new ErrorStorageAgentRequestHandler(this, "Input stream closed before the request was read");
                } else  if (transferState.readMessageType(channelInputBuffer, new StorageMessageHeaderCodec())) {
                    requestHandler = createRequestHandler();
                } else {
                    return Optional.empty();
                }
            } catch (IOException e) {
                LOG.error("Error while reading message type from {}", getRemoteAddress(), e);
                requestHandler = new ErrorStorageAgentRequestHandler(this, e.getMessage());
            }
        }
        return Optional.of(requestHandler);
    }

    private StorageAgentRequestHandler createRequestHandler() {
        String authenticationError;
        switch (transferState.getMessageType().getOperation()) {
            case PERSIST_DATA:
                logEvent(
                        "TCP_STREAM_STORAGE_DATA",
                        null,
                        ImmutableMap.<String, Object>of(
                                "callerIP", getRemoteAddress(),
                                "dataBundleId", transferState.getMessageType().getDataBundleId()
                        ));
                authenticationError = validateAuthentication();
                if (StringUtils.isBlank(authenticationError)) {
                    return new PersistStreamStorageAgentRequestHandler(this, agentStorageProxy, storageAllocatorService)
                            .initRequestHandler();
                } else {
                    return new ErrorStorageAgentRequestHandler(this, authenticationError);
                }
            case RETRIEVE_DATA:
                authenticationError = validateAuthentication();
                if (StringUtils.isBlank(authenticationError)) {
                    return new RetrieveStreamStorageAgentRequestHandler(this, agentStorageProxy)
                            .initRequestHandler();
                } else {
                    return new ErrorStorageAgentRequestHandler(this, authenticationError);
                }
            case PING:
                return new PingStorageAgentRequestHandler(this);
            default:
                LOG.error("Invalid operation {} from {}", transferState.getMessageType(), getRemoteAddress());
                return new ErrorStorageAgentRequestHandler(this, "Invalid operation " + transferState.getMessageType().toString());
        }
    }

    private void logEvent(String eventName, String eventDescription, Map<String, Object> eventData) {
        try {
            storageEventLogger.logStorageEvent(
                    eventName,
                    eventDescription,
                    eventData);
        } catch (Exception e) {
            LOG.warn("Error while trying to log storage event", e);
        }
    }
    private String validateAuthentication() {
        try {
            jacsCredentials = authTokenValidator.validateToken(transferState.getMessageType().getAuthToken(), null);
            return null;
        } catch (Exception e) {
            LOG.warn("Token validation exception {}", transferState.getMessageType().getAuthToken(), e);
            return e.getMessage();
        }
    }

    void writeResponse(StorageMessageResponse response) {
        try {
            TransferState<StorageMessageResponse> responseTransfer = new TransferState<>();
            byte[] responseBytes = responseTransfer.writeMessageType(response, new StorageMessageResponseCodec());
            writeBuffer(ByteBuffer.wrap(responseBytes));
        } catch (Exception e) {
            LOG.warn("Error sending error response to {}", getRemoteAddress(), e);
        }
    }

    void writeOutputBuffer() {
        try {
            writeBuffer(channelOutputBuffer);
        } catch (IOException e) {
            LOG.warn("Error sending data to {}", getRemoteAddress(), e);
        }
    }

    private void writeBuffer(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) socketChannel.write(buffer);
    }

    String getRemoteAddress() {
        try {
            return socketChannel.getRemoteAddress().toString();
        } catch (Exception e) {
            LOG.warn("Error while trying to get remote address from {}", socketChannel, e);
            return "<unknown>";
        }
    }

    long incrementNTransferredBytes(int nTransferredBytes) {
        this.nTransferredBytes += nTransferredBytes;
        return this.nTransferredBytes;
    }

    void initSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    void shutdownInput() {
        try {
            socketChannel = socketChannel.shutdownInput();
        } catch (Exception e) {
            LOG.warn("Error while trying to shutdown the input from {}", socketChannel, e);
        }
    }

    void switchChannelToWriteMode() {
        selectionKey.interestOps(SelectionKey.OP_WRITE);
    }
}
