package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class StorageAgentListener {

    private final String bindingIP;
    private final int portNo;
    private final ExecutorService agentExecutor;
    private final DataBundleIOProvider dataIOProvider;
    private final Map<SocketChannel, StorageAgent> socketStorageAgents = new HashMap<>();
    private final Logger logger;

    private Selector selector;

    @Inject
    public StorageAgentListener(@PropertyValue(name = "StorageAgent.bindingIP") String bindingIP,
                                @PropertyValue(name = "StorageAgent.portNo") int portNo,
                                ExecutorService agentExecutor,
                                DataBundleIOProvider dataIOProvider,
                                Logger logger) {
        this.bindingIP = bindingIP;
        this.portNo = portNo;
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
        this.logger = logger;
    }

    public void startServer() throws Exception {
        selector = Selector.open();

        ServerSocketChannel agentSocket = ServerSocketChannel.open();
        agentSocket.configureBlocking(false);

        InetSocketAddress agentAddr = new InetSocketAddress(bindingIP, portNo);
        agentSocket.socket().bind(agentAddr);

        agentSocket.register(selector, SelectionKey.OP_ACCEPT, null);
        logger.info("Started an agent listener at {}:{}", bindingIP, portNo);

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
        socketStorageAgents.put(channel, new StorageAgentImpl(agentExecutor, dataIOProvider));
        // register channel with selector for further IO
        channel.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        StorageAgent storageAgent = socketStorageAgents.get(channel);
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        int numRead = channel.read(buffer);
        buffer.flip();
        if (storageAgent.getState() == StorageAgentImpl.StorageAgentState.IDLE ||
                storageAgent.getState() == StorageAgentImpl.StorageAgentState.READ_HEADER) {
            if (numRead == -1) {
                socketStorageAgents.remove(channel);
                channel.close();
                key.cancel();
                throw new IllegalStateException("Stream closed before reading the agent command");
            }
            storageAgent.readHeader(buffer);
        }
        if (storageAgent.getState() == StorageAgentImpl.StorageAgentState.READ_DATA) {
            key.interestOps(SelectionKey.OP_WRITE);
            buffer.clear();
            return;
        } else if (storageAgent.getState() == StorageAgentImpl.StorageAgentState.WRITE_DATA) {
            if (numRead == -1) {
                socketStorageAgents.remove(channel);
                channel.close();
                key.cancel();
                buffer.clear();
                storageAgent.endWritingData(null);
                return;
            }
            storageAgent.writeData(buffer);
            buffer.clear();
        }
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        StorageAgent storageAgent = socketStorageAgents.get(channel);
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        while(storageAgent.readData(buffer) != -1) {
            buffer.flip();
            while (buffer.hasRemaining()) channel.write(buffer);
            buffer.clear();
        }
        key.interestOps(SelectionKey.OP_READ);
    }

}
