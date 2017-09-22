package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.StorageProtocol;
import org.janelia.jacsstorage.protocol.StorageProtocolImpl;
import org.janelia.jacsstorage.protocol.StorageMessageHeader;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;
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
import java.util.concurrent.ExecutorService;

public class StorageAgentListener {
    private final static Logger LOG = LoggerFactory.getLogger(StorageAgentListener.class);

    private static class ChannelState {
        private final ByteBuffer readBuffer;
        private final StorageProtocol serverProxy;
        private StorageMessageHeader request;
        private ByteBuffer responseBuffer;
        private long nTransferredBytes;
        ChannelState(ByteBuffer readBuffer, StorageProtocol serverProxy) {
            this.readBuffer = readBuffer;
            this.serverProxy = serverProxy;
            nTransferredBytes = 0L;
        }
    }

    private final String bindingIP;
    private final int portNo;
    private final ExecutorService agentExecutor;
    private final DataBundleIOProvider dataIOProvider;

    private Selector selector;
    private boolean running;

    @Inject
    public StorageAgentListener(@PropertyValue(name = "StorageAgent.bindingIP") String bindingIP,
                                @PropertyValue(name = "StorageAgent.portNo") int portNo,
                                ExecutorService agentExecutor,
                                DataBundleIOProvider dataIOProvider) {
        this.bindingIP = bindingIP;
        this.portNo = portNo;
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
    }

    public String open() throws IOException {
        selector = Selector.open();

        ServerSocketChannel agentSocketChannel = ServerSocketChannel.open();
        agentSocketChannel.configureBlocking(false);

        InetSocketAddress agentAddr = new InetSocketAddress(bindingIP, portNo);
        ServerSocket serverSocket = agentSocketChannel.socket();
        serverSocket.bind(agentAddr);
        agentSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        LOG.info("Started an agent listener on {}:{}", bindingIP, portNo);
        return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    public void startServer() throws IOException {
        // keep listener running
        boolean running = true;
        while (running) {
            // selects a set of keys whose corresponding channels are ready for I/O operations
            int readyChannels = selector.select();
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
        channel.register(this.selector, SelectionKey.OP_READ, new ChannelState(channelBuffer, new StorageProtocolImpl(agentExecutor, dataIOProvider)));
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelState channelState = (ChannelState) key.attachment();
        int numRead = 0;
        if (!channelState.readBuffer.hasRemaining()) {
            channelState.readBuffer.clear();
            numRead = channel.read(channelState.readBuffer);
            channelState.readBuffer.flip();
        }
        if (channelState.request == null) {
            if (numRead == -1) {
                // stream closed - still attempt to write the response and then close the channel
                LOG.error("Stream closed before reading the request");
                byte[] response = channelState.serverProxy.encodeRequest(new StorageMessageHeader(
                        StorageProtocol.Operation.PROCESS_ERROR,
                        JacsStorageFormat.ARCHIVE_DATA_FILE,
                        "Stream closed before the request was read"));
                try {
                    writeBuffer(ByteBuffer.wrap(response), channel);
                } catch (IOException e) {
                    LOG.warn("Error writing the sending the incomplete request message to the other end: {}", channel.getRemoteAddress());
                }
                channel.close();
                return;
            }
            StorageProtocol.Holder<StorageMessageHeader> requestHolder = new StorageProtocol.Holder<>();
            if (channelState.serverProxy.readRequest(channelState.readBuffer, requestHolder)) {
                channelState.request = requestHolder.getData();
                channelState.serverProxy.beginDataTransfer(channelState.request);
            } else {
                // request not fully read yet
                return;
            }
        }
        if (channelState.request.getOperation() == StorageProtocol.Operation.PERSIST_DATA) {
            if (numRead == -1) {
                // nothing left to read
                channelState.serverProxy.endDataTransfer();
                // prepare to write the response
                key.interestOps(SelectionKey.OP_WRITE);
                channelState.readBuffer.clear();
            } else {
                // persist the data
                channelState.nTransferredBytes  += channelState.serverProxy.writeData(channelState.readBuffer);
            }
        } else if (channelState.request.getOperation() == StorageProtocol.Operation.RETRIEVE_DATA) {
            // prepare to send the data
            key.interestOps(SelectionKey.OP_WRITE);
        } else {
            LOG.error("Invalid operation {} sent by {}", channelState.request.getOperation(), channel.getRemoteAddress());
            byte[] response = channelState.serverProxy.encodeRequest(new StorageMessageHeader(
                    StorageProtocol.Operation.PROCESS_ERROR,
                    JacsStorageFormat.ARCHIVE_DATA_FILE,
                    "Invalid operation"));
            try {
                writeBuffer(ByteBuffer.wrap(response), channel);
            } catch (IOException e) {
                LOG.warn("Error writing the sending invalid operation message to the other end: {}", channel.getRemoteAddress());
            }
            channel.close();
            return;
        }
    }

    private void write(SelectionKey key) throws IOException {
        byte[] responseBytes;
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelState channelState = (ChannelState) key.attachment();

        if (channelState.request.getOperation() == StorageProtocol.Operation.PERSIST_DATA) {
            switch (channelState.serverProxy.getState()) {
                case WRITE_DATA:
                    // still writing the data to the file system
                    return;
                case WRITE_DATA_COMPLETE:
                case WRITE_DATA_ERROR:
                    // write the response
                    responseBytes = channelState.serverProxy.encodeResponse(
                            new StorageMessageResponse(
                                    channelState.serverProxy.getState() == StorageProtocol.State.WRITE_DATA_COMPLETE ? 1 : 0,
                                    channelState.serverProxy.getLastErrorMessage(),
                                    channelState.nTransferredBytes));
                    writeBuffer(ByteBuffer.wrap(responseBytes), channel);
                    channel.close();
                    return;
                default:
                    LOG.warn("Invalid state {} while persisting data from {}", channelState.serverProxy.getState(), channel.getRemoteAddress());
                    channel.close();
                    throw new IllegalStateException("Invalid state while persisting data");
            }
        } else if (channelState.request.getOperation() == StorageProtocol.Operation.RETRIEVE_DATA) {
            switch (channelState.serverProxy.getState()) {
                case READ_DATA_STARTED:
                    return;
                case READ_DATA:
                case READ_DATA_COMPLETE:
                    if (channelState.responseBuffer == null) {
                        channelState.responseBuffer = ByteBuffer.allocate(2048);
                        int nbytes = channelState.serverProxy.readData(channelState.responseBuffer);
                        if (nbytes == -1) {
                            // nothing read
                            byte[] responseHeaderBytes;
                            if (channelState.serverProxy.getState() == StorageProtocol.State.READ_DATA_ERROR) {
                                responseHeaderBytes = channelState.serverProxy.encodeRequest(
                                        new StorageMessageHeader(StorageProtocol.Operation.PROCESS_ERROR, JacsStorageFormat.ARCHIVE_DATA_FILE, channelState.serverProxy.getLastErrorMessage())
                                );
                            } else {
                                responseHeaderBytes = channelState.serverProxy.encodeRequest(
                                        new StorageMessageHeader(StorageProtocol.Operation.PROCESS_RESPONSE, JacsStorageFormat.ARCHIVE_DATA_FILE, "")
                                );
                            }
                            writeBuffer(ByteBuffer.wrap(responseHeaderBytes), channel);
                            channel.close();
                            return;
                        } else {
                            channelState.responseBuffer.flip();
                            byte[] responseHeaderBytes = channelState.serverProxy.encodeRequest(
                                    new StorageMessageHeader(StorageProtocol.Operation.PROCESS_RESPONSE, JacsStorageFormat.ARCHIVE_DATA_FILE, "")
                            );
                            writeBuffer(ByteBuffer.wrap(responseHeaderBytes), channel);
                        }
                    }
                    if (!channelState.responseBuffer.hasRemaining()) {
                        channelState.responseBuffer.clear();
                        // get more data to send
                        if (channelState.serverProxy.readData(channelState.responseBuffer) == -1) {
                            // nothing more to send so simply close the channel since sending the data will not have a response suffix
                            channel.close();
                            return;
                        }
                        channelState.responseBuffer.flip();
                    }
                    writeBuffer(channelState.responseBuffer, channel);
                    return;
                case READ_DATA_ERROR:
                    if (channelState.responseBuffer == null) {
                        byte[] response = channelState.serverProxy.encodeRequest(new StorageMessageHeader(
                                StorageProtocol.Operation.PROCESS_ERROR,
                                JacsStorageFormat.ARCHIVE_DATA_FILE,
                                channelState.serverProxy.getLastErrorMessage()));
                        writeBuffer(ByteBuffer.wrap(response), channel);
                    } // else it already sent the response header so just close the channel
                    channel.close();
                    return;
                default:
                    LOG.warn("Invalid state {} while persisting data from {}", channelState.serverProxy.getState(), channel.getRemoteAddress());
                    channel.close();
                    return;
            }
        } else {
            throw new IllegalStateException("Invalid operation " + channelState.request.getOperation());
        }
    }

    private void writeBuffer(ByteBuffer buffer, SocketChannel channel) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }
}
