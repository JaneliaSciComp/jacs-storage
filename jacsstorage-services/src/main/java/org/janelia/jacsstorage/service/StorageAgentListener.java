package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.State;
import org.janelia.jacsstorage.protocol.StorageMessageHeaderCodec;
import org.janelia.jacsstorage.protocol.StorageMessageResponseCodec;
import org.janelia.jacsstorage.protocol.StorageService;
import org.janelia.jacsstorage.protocol.StorageMessageHeader;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;
import org.janelia.jacsstorage.protocol.TransferState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class StorageAgentListener {
    private final static Logger LOG = LoggerFactory.getLogger(StorageAgentListener.class);
    private static final long SELECTOR_TIMEOUT = 10000;

    private static class ChannelState {
        private final ByteBuffer channelInputBuffer;
        private final TransferState<StorageMessageHeader> transferState;
        private ByteBuffer channelOutputBuffer;
        private long nTransferredBytes;
        ChannelState(ByteBuffer channelInputBuffer) {
            this.channelInputBuffer = channelInputBuffer;
            this.transferState = new TransferState<>();
            nTransferredBytes = 0;
        }
    }

    private final String bindingIP;
    private final int portNo;
    private final StorageService agentStorageProxy;

    private Selector selector;
    private boolean running;

    @Inject
    public StorageAgentListener(@PropertyValue(name = "StorageAgent.bindingIP") String bindingIP,
                                @PropertyValue(name = "StorageAgent.portNo") int portNo,
                                @PooledResource StorageService agentStorageProxy) {
        this.bindingIP = bindingIP;
        this.portNo = portNo;
        this.agentStorageProxy = agentStorageProxy;
    }

    public String open() throws IOException {
        selector = Selector.open();

        ServerSocketChannel agentSocketChannel = ServerSocketChannel.open();
        agentSocketChannel.configureBlocking(false);

        InetSocketAddress agentAddr = new InetSocketAddress(bindingIP, portNo);
        ServerSocket serverSocket = agentSocketChannel.socket();
        serverSocket.bind(agentAddr);
        agentSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        LOG.info("Started an agent listener on {}:{} ({})", bindingIP, portNo, serverSocket.getLocalSocketAddress());
        return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    public void startServer() throws IOException {
        // keep listener running
        boolean running = true;
        while (running) {
            // selects a set of keys whose corresponding channels are ready for I/O operations
            int readyChannels = selector.select(SELECTOR_TIMEOUT);
            if (readyChannels == 0) continue;

            Iterator<SelectionKey> agentKeys = selector.selectedKeys().iterator();

            while (agentKeys.hasNext()) {
                SelectionKey currentKey = agentKeys.next();
                agentKeys.remove();
                if (!currentKey.isValid()) {
                    continue;
                }
                if (currentKey.isAcceptable()) {
                    // key is ready to accept a new socket connection
                    accept(currentKey);
                } else if (currentKey.isReadable()) {
                    // key is ready for reading
                    read(currentKey);
                } else if (currentKey.isWritable()) {
                    write(currentKey);
                }
            }
        }
    }

    public void stopServer() throws IOException {
        running = false;
        if (selector != null) selector.close();
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        LOG.debug("Accept incoming connection from {}", channel.getRemoteAddress());
        // register channel with selector for further IO
        ByteBuffer channelBuffer = ByteBuffer.allocate(2048);
        channelBuffer.limit(0); // empty
        channel.register(this.selector, SelectionKey.OP_READ, new ChannelState(channelBuffer));
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelState channelState = (ChannelState) key.attachment();
        int numRead = 0;
        if (!channelState.channelInputBuffer.hasRemaining()) {
            channelState.channelInputBuffer.clear();
            numRead = channel.read(channelState.channelInputBuffer);
            channelState.channelInputBuffer.flip();
        }
        if (!channelState.transferState.hasReadEntireMessageType()) {
            if (numRead == -1) {
                // stream closed - still attempt to write the response and then close the channel
                LOG.error("Stream closed before reading the request");
                StorageMessageHeader processErrorResponse = new StorageMessageHeader(
                        StorageService.Operation.PROCESS_ERROR,
                        JacsStorageFormat.ARCHIVE_DATA_FILE,
                        "",
                        "Stream closed before the request was read");
                TransferState<StorageMessageHeader> responseTransfer = new TransferState<>();
                byte[] processErrorResponseBytes = responseTransfer.writeMessageType(processErrorResponse, new StorageMessageHeaderCodec());
                try {
                    writeBuffer(ByteBuffer.wrap(processErrorResponseBytes), channel);
                } catch (IOException e) {
                    LOG.warn("Error writing the sending the incomplete request message to the other end: {}", channel.getRemoteAddress());
                }
                channel.close();
                return;
            }
            if (channelState.transferState.readMessageType(channelState.channelInputBuffer, new StorageMessageHeaderCodec())) {
                agentStorageProxy.beginDataTransfer(channelState.transferState);
            } else {
                // message header not fully read
                return;
            }
        }
        if (channelState.transferState.getMessageType().getOperation() == StorageService.Operation.PERSIST_DATA) {
            if (numRead == -1) {
                // nothing left to read
                // prepare to write the response
                key.interestOps(SelectionKey.OP_WRITE);
                try {
                    agentStorageProxy.endDataTransfer(channelState.transferState);
                } finally {
                    channel.shutdownInput();
                }
            } else {
                // persist the data
                int numWritten = agentStorageProxy.writeData(channelState.channelInputBuffer, channelState.transferState);
                if (numWritten == -1) {
                    key.interestOps(SelectionKey.OP_WRITE);
                    channel.shutdownInput();
                } else {
                    channelState.nTransferredBytes += numWritten;
                }
            }
        } else if (channelState.transferState.getMessageType().getOperation() == StorageService.Operation.RETRIEVE_DATA) {
            // prepare to send the data
            key.interestOps(SelectionKey.OP_WRITE);
        } else {
            key.interestOps(SelectionKey.OP_WRITE);
            LOG.error("Invalid operation {} sent by {}", channelState.transferState.getMessageType().getOperation(), channel.getRemoteAddress());
            StorageMessageHeader invalidOperationResponse = new StorageMessageHeader(
                    StorageService.Operation.PROCESS_ERROR,
                    JacsStorageFormat.ARCHIVE_DATA_FILE,
                    "",
                    "Invalid operation " + channelState.transferState.getMessageType().getOperation());
            TransferState<StorageMessageHeader> responseTransfer = new TransferState<>();
            byte[] invalidOperationResponseBytes = responseTransfer.writeMessageType(invalidOperationResponse, new StorageMessageHeaderCodec());
            try {
                writeBuffer(ByteBuffer.wrap(invalidOperationResponseBytes), channel);
            } catch (IOException e) {
                LOG.warn("Error writing the sending invalid operation {} message to the other end: {}", channelState.transferState.getMessageType().getOperation(), channel.getRemoteAddress());
            }
            channel.close();
            return;
        }
    }

    private void write(SelectionKey key) throws IOException {
        byte[] responseBytes;
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelState channelState = (ChannelState) key.attachment();
        if (channelState.transferState.getMessageType().getOperation() == StorageService.Operation.PERSIST_DATA) {
            StorageMessageResponse response;
            TransferState<StorageMessageResponse> responseTransfer;
            switch (channelState.transferState.getState()) {
                case WRITE_DATA_STARTED:
                case WRITE_DATA:
                    // still writing the data to the file system
                    return;
                case WRITE_DATA_COMPLETE:
                case WRITE_DATA_ERROR:
                    // write the response
                    response = new StorageMessageResponse(channelState.transferState.getState() == State.WRITE_DATA_COMPLETE ? StorageMessageResponse.OK : StorageMessageResponse.ERROR,
                            channelState.transferState.getErrorMessage(),
                            channelState.nTransferredBytes,
                            channelState.transferState.getPersistedBytes());
                    responseTransfer = new TransferState<>();
                    responseBytes = responseTransfer.writeMessageType(response, new StorageMessageResponseCodec());
                    writeBuffer(ByteBuffer.wrap(responseBytes), channel);
                    channel.close();
                    return;
                default:
                    LOG.warn("Invalid state {} while persisting data from {}", channelState.transferState.getState(), channel.getRemoteAddress());
                    response = new StorageMessageResponse(StorageMessageResponse.ERROR,
                            "Invalid state " + channelState.transferState.getState() + "while persisting data from " + channel.getRemoteAddress(),
                            channelState.nTransferredBytes,
                            channelState.transferState.getPersistedBytes());
                    responseTransfer = new TransferState<>();
                    responseBytes = responseTransfer.writeMessageType(response, new StorageMessageResponseCodec());
                    writeBuffer(ByteBuffer.wrap(responseBytes), channel);
                    channel.close();
            }
        } else if (channelState.transferState.getMessageType().getOperation() == StorageService.Operation.RETRIEVE_DATA) {
            StorageMessageHeader responseHeader;
            TransferState<StorageMessageHeader> responseHeaderTransfer;
            byte[] responseHeaderBytes;
            switch (channelState.transferState.getState()) {
                case READ_DATA_STARTED:
                    return;
                case READ_DATA:
                case READ_DATA_COMPLETE:
                    if (channelState.channelOutputBuffer == null) {
                        // no response has been sent yet
                        ByteBuffer responseBuffer = ByteBuffer.allocate(2048);
                        int nbytes = agentStorageProxy.readData(responseBuffer, channelState.transferState);
                        if (nbytes == -1) {
                            // nothing read from the data channel
                            if (channelState.transferState.getState() == State.READ_DATA_ERROR) {
                                responseHeader = new StorageMessageHeader(
                                        StorageService.Operation.PROCESS_ERROR,
                                        JacsStorageFormat.ARCHIVE_DATA_FILE,
                                        "", // the client is responsible for deciding where to save
                                        channelState.transferState.getErrorMessage());
                            } else {
                                responseHeader = new StorageMessageHeader(
                                        StorageService.Operation.PROCESS_RESPONSE,
                                        JacsStorageFormat.ARCHIVE_DATA_FILE,
                                        "",
                                        channelState.transferState.getErrorMessage());
                            }
                            responseHeaderTransfer = new TransferState<>();
                            responseHeaderBytes = responseHeaderTransfer.writeMessageType(responseHeader, new StorageMessageHeaderCodec());
                            writeBuffer(ByteBuffer.wrap(responseHeaderBytes), channel);
                            channel.close();
                            return;
                        } else if (nbytes > 0) {
                            channelState.channelOutputBuffer = responseBuffer;
                            channelState.channelOutputBuffer.flip();
                            responseHeader = new StorageMessageHeader(
                                    StorageService.Operation.PROCESS_RESPONSE,
                                    JacsStorageFormat.ARCHIVE_DATA_FILE,
                                    "",
                                    channelState.transferState.getErrorMessage());
                            responseHeaderTransfer = new TransferState<>();
                            responseHeaderBytes = responseHeaderTransfer.writeMessageType(responseHeader, new StorageMessageHeaderCodec());
                            writeBuffer(ByteBuffer.wrap(responseHeaderBytes), channel);
                        }
                    } else {
                        // the response header was already sent so continue with sending the data
                        if (!channelState.channelOutputBuffer.hasRemaining()) {
                            channelState.channelOutputBuffer.clear();
                            // get more data to send
                            if (agentStorageProxy.readData(channelState.channelOutputBuffer, channelState.transferState) == -1) {
                                // nothing more to send so simply close the channel since sending the data will not have a response suffix
                                channel.close();
                                return;
                            }
                            channelState.channelOutputBuffer.flip();
                        }
                        writeBuffer(channelState.channelOutputBuffer, channel);
                    }
                    return;
                case READ_DATA_ERROR:
                    if (channelState.channelOutputBuffer == null) {
                        responseHeader = new StorageMessageHeader(
                                StorageService.Operation.PROCESS_ERROR,
                                JacsStorageFormat.ARCHIVE_DATA_FILE,
                                "",
                                channelState.transferState.getErrorMessage());
                        responseHeaderTransfer = new TransferState<>();
                        responseHeaderBytes = responseHeaderTransfer.writeMessageType(responseHeader, new StorageMessageHeaderCodec());
                        writeBuffer(ByteBuffer.wrap(responseHeaderBytes), channel);
                    } // else it already sent the response header so just close the channel
                    channel.close();
                    return;
                default:
                    LOG.warn("Invalid state {} while persisting data from {}", channelState.transferState.getState(), channel.getRemoteAddress());
                    return;
            }
        } else {
            throw new IllegalStateException("Invalid operation " + channelState.transferState.getMessageType().getOperation());
        }
    }

    private void writeBuffer(ByteBuffer buffer, SocketChannel channel) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }
}
