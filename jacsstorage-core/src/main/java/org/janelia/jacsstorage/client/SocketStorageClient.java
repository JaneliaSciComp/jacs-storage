package org.janelia.jacsstorage.client;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.protocol.StorageProtocol;

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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class SocketStorageClient implements StorageClient {

    private final StorageProtocol localAgentProxy;
    private final StorageProtocol remoteAgentProxy;
    private Selector selector;

    public SocketStorageClient(StorageProtocol localAgentProxy, StorageProtocol remoteAgentProxy) {
        this.localAgentProxy = localAgentProxy;
        this.remoteAgentProxy = remoteAgentProxy;
    }

    @Override
    public void persistData(String localPath, DataStorageInfo storageInfo) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = localAgentProxy.createHeader(StorageProtocol.Operation.PERSIST_DATA,
                storageInfo.getStorageFormat(),
                storageInfo.getPath());
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
        byte[] localHeaderBytes = localAgentProxy.createHeader(StorageProtocol.Operation.RETRIEVE_DATA,
                localDataFormat,
                localPath);
        ByteBuffer localHeaderBuffer = ByteBuffer.wrap(localHeaderBytes);
        if (localAgentProxy.readHeader(localHeaderBuffer, Optional.empty()) == 1) {
            localAgentProxy.beginDataTransfer();
        }

        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        dataTransferBuffer.limit(0); // the data buffer is empty
        sendData(dataTransferBuffer);
    }

    public void retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = remoteAgentProxy.createHeader(StorageProtocol.Operation.RETRIEVE_DATA,
                storageInfo.getStorageFormat(),
                storageInfo.getPath());
        ByteBuffer remoteOpBuffer = ByteBuffer.wrap(remoteOpBytes);
        openChannel(remoteOpBuffer, getConnectionHost(storageInfo.getConnectionInfo()), getConnectionPort(storageInfo.getConnectionInfo()), SelectionKey.OP_READ); // open the channel for reading the data

        // figure out how to write the local data
        JacsStorageFormat localDataFormat;
        if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
            localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        } else {
            // expand everything locally
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        }
        // initiate the local data write operation
        byte[] localHeaderBytes = localAgentProxy.createHeader(StorageProtocol.Operation.PERSIST_DATA,
                localDataFormat,
                localPath);
        ByteBuffer localHeaderBuffer = ByteBuffer.wrap(localHeaderBytes);
        if (localAgentProxy.readHeader(localHeaderBuffer, Optional.empty()) == 1) {
            localAgentProxy.beginDataTransfer();
        }

        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        retrieveData(dataTransferBuffer);
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
        sendHeader(headerBuffer, channelIOOp);
    }

    private void sendHeader(ByteBuffer headerBuffer, int nextOp) throws IOException {
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
                            key.cancel();
                            channel.close();
                            return;
                        } else {
                            dataBuffer.flip(); // prepare the buffer for reading again
                        }
                    }
                }
            }
        }
    }

    private void retrieveData(ByteBuffer dataBuffer) throws IOException {
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
                    if (read(key, dataBuffer) == -1) {
                        return;
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

    private int read (SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        int numRead = channel.read(buffer);
        buffer.flip();
        if (remoteAgentProxy.getState() == StorageProtocol.State.IDLE ||
                remoteAgentProxy.getState() == StorageProtocol.State.READ_HEADER) {
            if (numRead == -1) {
                channel.close();
                key.cancel();
                throw new IllegalStateException("Stream closed before reading the agent command");
            }
            remoteAgentProxy.readHeader(buffer, Optional.of(StorageProtocol.State.READ_DATA));
        }
        if (remoteAgentProxy.getState() == StorageProtocol.State.READ_DATA) {
            if (numRead == -1) {
                // done so tell the local agent we are done and and wait for it to complete as well
                CountDownLatch done = new CountDownLatch(1);
                localAgentProxy.terminateDataTransfer(() -> done.countDown());
                try {
                    done.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                } finally {
                    key.cancel();
                    channel.close();
                }
            } else {
                localAgentProxy.writeData(buffer);
            }
        } else if (remoteAgentProxy.getState() == StorageProtocol.State.WRITE_DATA) {
            channel.close();
            key.cancel();
            throw new IllegalStateException("The remote operation cannot be a write");
        }
        buffer.clear();
        return numRead;
    }

    private int write(SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        return channel.write(buffer);
    }

}
