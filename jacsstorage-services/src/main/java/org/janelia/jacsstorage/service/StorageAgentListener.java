package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.io.DataBundleIOProvider;

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
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int numRead = channel.read(buffer);
        if (numRead == -1) {
            socketStorageAgents.remove(channel);
            channel.close();
            key.cancel();
            return;
        }
    }
}
