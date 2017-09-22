package org.janelia.jacsstorage.client;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.StorageProtocol;
import org.janelia.jacsstorage.protocol.StorageMessageHeader;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class SocketStorageClient implements StorageClient {

    private final StorageProtocol localAgentProxy;
    private final StorageProtocol remoteAgentProxy;
    private Selector selector;

    public SocketStorageClient(StorageProtocol localAgentProxy, StorageProtocol remoteAgentProxy) {
        this.localAgentProxy = localAgentProxy;
        this.remoteAgentProxy = remoteAgentProxy;
    }

    @Override
    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = localAgentProxy.encodeRequest(new StorageMessageHeader(
                StorageProtocol.Operation.PERSIST_DATA,
                storageInfo.getStorageFormat(),
                storageInfo.getPath()));
        ByteBuffer remoteOpBuffer = ByteBuffer.wrap(remoteOpBytes);
        openChannel(remoteOpBuffer, getConnectionHost(storageInfo.getConnectionInfo()), getConnectionPort(storageInfo.getConnectionInfo()), SelectionKey.OP_WRITE); // open the channel for writing the data

        // figure out the best local data format
        Path sourcePath = Paths.get(localPath);
        JacsStorageFormat localDataFormat;
        if (Files.isDirectory(sourcePath)) {
            if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
                throw new IllegalArgumentException("Cannot persist directory " + localPath + " as a single file");
            }
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        } else {
            if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
                localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
            } else {
                localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
            }
        }
        // initiate the local data read operation
        localAgentProxy.beginDataTransfer(new StorageMessageHeader(
                StorageProtocol.Operation.RETRIEVE_DATA,
                localDataFormat,
                localPath));

        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        dataTransferBuffer.limit(0); // the data buffer is empty
        sendData(dataTransferBuffer);
        dataTransferBuffer.position(0);
        dataTransferBuffer.limit(0);
        return retrieveResponse(dataTransferBuffer);
    }

    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = remoteAgentProxy.encodeRequest(new StorageMessageHeader(
                StorageProtocol.Operation.RETRIEVE_DATA,
                storageInfo.getStorageFormat(),
                storageInfo.getPath()));
        ByteBuffer remoteOpBuffer = ByteBuffer.wrap(remoteOpBytes);
        openChannel(remoteOpBuffer, getConnectionHost(storageInfo.getConnectionInfo()), getConnectionPort(storageInfo.getConnectionInfo()), SelectionKey.OP_READ); // open the channel for reading the data

        // figure out how to write the local data
        JacsStorageFormat localDataFormat;
        if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
            Files.createDirectories(Paths.get(localPath).getParent());
            localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        } else {
            // expand everything locally
            Files.createDirectories(Paths.get(localPath));
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        }
        // initiate the local data write operation
        localAgentProxy.beginDataTransfer(new StorageMessageHeader(
                StorageProtocol.Operation.PERSIST_DATA,
                localDataFormat,
                localPath));
        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        dataTransferBuffer.position(0);
        dataTransferBuffer.limit(0);
        StorageMessageHeader responseHeader = retrieveResponseHeader(dataTransferBuffer);
        if (responseHeader.getOperation() == StorageProtocol.Operation.PROCESS_RESPONSE) {
            return retrieveData(dataTransferBuffer);
        } else if (responseHeader.getOperation() == StorageProtocol.Operation.PROCESS_ERROR) {
            return new StorageMessageResponse(0, responseHeader.getLocation(), 0);
        } else {
            throw new IllegalStateException("Invalid response operation");
        }
    }

    private String getConnectionHost(String connectionInfo) {
        if (StringUtils.isNotBlank(connectionInfo)) {
            int separatorIndex = connectionInfo.indexOf(':');
            if (separatorIndex != -1) {
                return connectionInfo.substring(0, separatorIndex).trim();
            } else {
                return connectionInfo;
            }
        } else {
            return null;
        }
    }

    private int getConnectionPort(String connectionInfo) {
        if (StringUtils.isNotBlank(connectionInfo)) {
            int separatorIndex = connectionInfo.indexOf(':');
            if (separatorIndex != -1) {
                return Integer.parseInt(connectionInfo.substring(separatorIndex + 1).trim());
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    private void openChannel(ByteBuffer headerBuffer, String serverAddress, int serverPort, int channelIOOp) throws IOException {
        SocketChannel channel;
        selector = Selector.open();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_CONNECT);
        channel.connect(new InetSocketAddress(serverAddress, serverPort));
        sendRequest(headerBuffer, channelIOOp);
    }

    private void sendRequest(ByteBuffer headerBuffer, int nextOp) throws IOException {
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isConnectable()) {
                    connect(key);
                }
                if (key.isWritable()) {
                    if (headerBuffer.hasRemaining()) {
                        // keep writing until the entire header was written
                        write(key, headerBuffer);
                    } else {
                        key.interestOps(nextOp);
                        return;
                    }
                }
            }
        }
    }

    private void sendData(ByteBuffer dataBuffer) throws IOException {
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isWritable()) {
                    if (dataBuffer.hasRemaining()) {
                        write(key, dataBuffer);
                    } else {
                        // get more bytes to write
                        dataBuffer.clear();
                        int nbytes = localAgentProxy.readData(dataBuffer);
                        if (nbytes == -1) {
                            // done
                            SocketChannel channel = (SocketChannel) key.channel();
                            channel.shutdownOutput();
                            // get ready to read the response
                            key.interestOps(SelectionKey.OP_READ);
                            return;
                        } else {
                            dataBuffer.flip(); // prepare the buffer for reading again
                        }
                    }
                }
            }
        }
    }

    private StorageMessageResponse retrieveResponse(ByteBuffer dataBuffer) throws IOException {
        StorageProtocol.Holder<StorageMessageResponse> responseHolder = new StorageProtocol.Holder<>();
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    int numRead = read(channel, dataBuffer);
                    if (numRead == -1) {
                        channel.close();
                        return responseHolder.getData();
                    }
                    if (remoteAgentProxy.readResponse(dataBuffer, responseHolder)) {
                        channel.close();
                        return responseHolder.getData();
                    }
                }
            }
        }
    }

    private StorageMessageHeader retrieveResponseHeader(ByteBuffer dataBuffer) throws IOException {
        StorageProtocol.Holder<StorageMessageHeader> requestHolder = new StorageProtocol.Holder<>();
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (dataBuffer.hasRemaining()) {
                        if (remoteAgentProxy.readRequest(dataBuffer, requestHolder)) {
                            return requestHolder.getData();
                        }
                    } else {
                        int numRead = read(channel, dataBuffer);
                        if (numRead == -1) {
                            channel.close();
                            return requestHolder.getData();
                        }
                    }
                }
            }
        }
    }

    private StorageMessageResponse retrieveData(ByteBuffer dataBuffer) throws IOException {
        long nTransferredBytes = 0L;
        while (true) {
            selector.select();
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isReadable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (dataBuffer.hasRemaining()) {
                        nTransferredBytes += localAgentProxy.writeData(dataBuffer);
                    } else {
                        int numRead = read(channel, dataBuffer);
                        if (numRead == -1) {
                            try {
                                localAgentProxy.endDataTransfer();
                                while (localAgentProxy.getState() != StorageProtocol.State.WRITE_DATA_COMPLETE && localAgentProxy.getState() != StorageProtocol.State.WRITE_DATA_ERROR) {
                                        Thread.sleep(1); // wait until the data transfer is completed
                                }
                            } catch (InterruptedException e) {
                                break;
                            } finally {
                                channel.close();
                            }
                            return new StorageMessageResponse(1, null, nTransferredBytes);
                        }
                    }
                }
            }
        }
    }

    private void connect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_WRITE);
    }

    private int read(SocketChannel channel, ByteBuffer buffer) throws IOException {
        int nbytes = 0;
        if (!buffer.hasRemaining()) {
            buffer.clear();
            nbytes = channel.read(buffer);
            buffer.flip();
        }
        return nbytes;
    }

    private int write(SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        return channel.write(buffer);
    }

}
