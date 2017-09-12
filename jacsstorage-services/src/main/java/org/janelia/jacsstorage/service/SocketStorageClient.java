package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import javax.inject.Inject;
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
import java.util.concurrent.CountDownLatch;

public class SocketStorageClient implements StorageClient {

    private final StorageAgent localAgentProxy;
    private final StorageAgent remoteAgentProxy;
    private final String serverIP;
    private final int portNo;
    private Selector selector;

    public SocketStorageClient(StorageAgent localAgentProxy, StorageAgent remoteAgentProxy, String serverIP, int portNo) {
        this.localAgentProxy = localAgentProxy;
        this.remoteAgentProxy = remoteAgentProxy;
        this.serverIP = serverIP;
        this.portNo = portNo;
    }

    public void persistData(String source, String target, JacsStorageFormat remoteDataFormat) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = localAgentProxy.getHeaderBuffer(StorageAgent.StorageAgentOperation.PERSIST_DATA,
                remoteDataFormat,
                target);
        ByteBuffer remoteOpBuffer = ByteBuffer.wrap(remoteOpBytes);
        openChannel(remoteOpBuffer, SelectionKey.OP_WRITE); // open the channel for writing the data

        // figure out the best local data format
        Path sourcePath = Paths.get(source);
        JacsStorageFormat localDataFormat;
        if (Files.isDirectory(sourcePath)) {
            if (remoteDataFormat == JacsStorageFormat.SINGLE_DATA_FILE) {
                throw new IllegalArgumentException("Cannot persist directory " + source + " as a single file");
            }
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        } else {
            if (remoteDataFormat == JacsStorageFormat.SINGLE_DATA_FILE) {
                localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
            } else {
                localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
            }
        }
        // initiate the local data read operation
        byte[] localHeaderBytes = localAgentProxy.getHeaderBuffer(StorageAgent.StorageAgentOperation.RETRIEVE_DATA,
                localDataFormat,
                source);
        ByteBuffer localHeaderBuffer = ByteBuffer.wrap(localHeaderBytes);
        localAgentProxy.readData(localHeaderBuffer);

        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        dataTransferBuffer.limit(0); // the data buffer is empty
        sendData(dataTransferBuffer);
    }

    public void retrieveData(String source, String target, JacsStorageFormat remoteDataFormat) throws IOException {
        // initialize the transfer operation - tell the remote party what we want
        byte[] remoteOpBytes = localAgentProxy.getHeaderBuffer(StorageAgent.StorageAgentOperation.RETRIEVE_DATA,
                remoteDataFormat,
                source); // in this case the source is the remote path from where to get the data
        ByteBuffer remoteOpBuffer = ByteBuffer.wrap(remoteOpBytes);
        openChannel(remoteOpBuffer, SelectionKey.OP_READ); // open the channel for reading the data

        // figure out how to write the local data
        JacsStorageFormat localDataFormat;
        if (remoteDataFormat == JacsStorageFormat.SINGLE_DATA_FILE) {
            localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        } else {
            // expand everything locally
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        }
        // initiate the local data read operation
        byte[] localHeaderBytes = localAgentProxy.getHeaderBuffer(StorageAgent.StorageAgentOperation.PERSIST_DATA,
                localDataFormat,
                target);
        ByteBuffer localHeaderBuffer = ByteBuffer.wrap(localHeaderBytes);
        localAgentProxy.readData(localHeaderBuffer);

        ByteBuffer dataTransferBuffer = ByteBuffer.allocate(2048);
        dataTransferBuffer.limit(0); // the data buffer is empty
        retrieveData(dataTransferBuffer);
    }

    private void openChannel(ByteBuffer headerBuffer, int channelIOOp) throws IOException {
        SocketChannel channel;
        selector = Selector.open();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_CONNECT);
        channel.connect(new InetSocketAddress(serverIP, portNo));
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
        if (remoteAgentProxy.getState() == StorageAgentImpl.StorageAgentState.IDLE ||
                remoteAgentProxy.getState() == StorageAgentImpl.StorageAgentState.READ_HEADER) {
            if (numRead == -1) {
                channel.close();
                key.cancel();
                throw new IllegalStateException("Stream closed before reading the agent command");
            }
        }
        if (remoteAgentProxy.getState() == StorageAgentImpl.StorageAgentState.READ_DATA) {
            if (numRead == -1) {
                // done so tell the local agent we are done and and wait for it to complete as well
                CountDownLatch done = new CountDownLatch(1);
                localAgentProxy.endWritingData(() -> done.countDown());
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
            buffer.clear();
        } else if (remoteAgentProxy.getState() == StorageAgentImpl.StorageAgentState.WRITE_DATA) {
            channel.close();
            key.cancel();
            throw new IllegalStateException("The remote operation cannot be a write");
        }
        return numRead;
    }

    private int write(SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        return channel.write(buffer);
    }

}
