package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.io.DataBundleIOProvider;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

public class StorageAgentListener {

    private final String bindingIP;
    private final int portNo;
    private final Executor agentExecutor;
    private final DataBundleIOProvider dataIOProvider;
    private final Map<SocketChannel, StorageAgent> socketStorageAgents = new HashMap<>();

    private Selector selector;

    @Inject
    public StorageAgentListener(@PropertyValue(name = "StorageAgent.bindingIP") String bindingIP,
                                @PropertyValue(name = "StorageAgent.portNo") int portNo,
                                Executor agentExecutor,
                                DataBundleIOProvider dataIOProvider) {
        this.bindingIP = bindingIP;
        this.portNo = portNo;
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
    }

    public void startServer() throws Exception {
        selector = Selector.open();

        ServerSocketChannel agentSocket = ServerSocketChannel.open();
        agentSocket.configureBlocking(false);

        InetSocketAddress agentAddr = new InetSocketAddress(bindingIP, portNo);
        agentSocket.socket().bind(agentAddr);

        agentSocket.register(selector, SelectionKey.OP_ACCEPT, null);

        // keep listener running
        while (true) {
            // selects a set of keys whose corresponding channels are ready for I/O operations
            selector.select();
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

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel channel = serverChannel.accept();
        channel.configureBlocking(false);
        socketStorageAgents.put(channel, new StorageAgent(agentExecutor, dataIOProvider));
        // register channel with selector for further IO
        channel.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        StorageAgent storageAgent = socketStorageAgents.get(channel);
        int numRead;
        if (storageAgent.getState() == StorageAgent.StorageAgentState.IDLE) {
            ByteBuffer cmdSizeBuffer = storageAgent.getCmdSizeValueBuffer();
            int remainingCmdSizeBytes = cmdSizeBuffer.remaining();
            numRead = channel.read(cmdSizeBuffer);
            if (numRead == -1) {
                socketStorageAgents.remove(channel);
                channel.close();
                key.cancel();
                throw new IllegalStateException("Stream closed before reading the size of the agent command");
            } else if (numRead < remainingCmdSizeBytes) {
                return; // not enough bytes could be read from the channel to read the entire cmd size so we'll have to come back
            } else {
                storageAgent.beginReadingAction();
            }
        }
        if (storageAgent.getState() == StorageAgent.StorageAgentState.READ_ACTION) {
            ByteBuffer cmdBuffer = storageAgent.getCmdBuffer();
            int remainingCmdBytes = cmdBuffer.remaining();
            numRead = channel.read(cmdBuffer);
            if (numRead == -1) {
                socketStorageAgents.remove(channel);
                channel.close();
                key.cancel();
                throw new IllegalStateException("Stream closed before reading the agent command");
            } else if (numRead < remainingCmdBytes) {
                return; // not enough bytes could be read from the channel to read the entire cmd so we'll have to come back
            } else {
                storageAgent.readAction();
                if (storageAgent.getState() == StorageAgent.StorageAgentState.READ_DATA) {
                    key.interestOps(SelectionKey.OP_WRITE);
                    return;
                }
            }
        }
        if (storageAgent.getState() != StorageAgent.StorageAgentState.WRITE_DATA) {
            throw new IllegalStateException(); // the agent should be in write data state here
        }
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        numRead = channel.read(buffer);
        if (numRead == -1) {
            socketStorageAgents.remove(channel);
            channel.close();
            key.cancel();
            storageAgent.endWritingData();
            return;
        } else {
            buffer.flip();
            storageAgent.writeData(buffer.array(), buffer.position(), buffer.limit());
            buffer.clear();
        }
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        StorageAgent storageAgent = socketStorageAgents.get(channel);
        byte[] buffer = new byte[2048];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        int nbytes;
        while((nbytes = storageAgent.readData(buffer, 0, buffer.length)) != -1) {
            byteBuffer.position(0);
            byteBuffer.limit(nbytes);
            while (byteBuffer.hasRemaining()) channel.write(byteBuffer);
        }
        key.interestOps(SelectionKey.OP_READ);
    }

}
