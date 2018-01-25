package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PooledResource;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.security.JwtTokenCredentialsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class StorageAgentListener {
    private static final Logger LOG = LoggerFactory.getLogger(StorageAgentListener.class);
    private static final long SELECTOR_TIMEOUT = 10000;

    private final DataTransferService agentStorageProxy;
    private final StorageAllocatorService storageAllocatorService;
    private final JwtTokenCredentialsValidator authTokenValidator;
    private final StorageEventLogger storageEventLogger;

    private Selector selector;
    private boolean running;

    @Inject
    public StorageAgentListener(@PooledResource DataTransferService agentStorageProxy,
                                @LocalInstance StorageAllocatorService storageAllocatorService,
                                @PropertyValue(name = "JWT.SecretKey") String authKey,
                                StorageEventLogger storageEventLogger) {
        this.agentStorageProxy = agentStorageProxy;
        this.storageAllocatorService = storageAllocatorService;
        this.authTokenValidator = new JwtTokenCredentialsValidator(authKey);
        this.storageEventLogger = storageEventLogger;
    }

    public int open(String bindingIP, int portNo) throws IOException {
        selector = Selector.open();

        ServerSocketChannel agentSocketChannel = ServerSocketChannel.open();
        agentSocketChannel.configureBlocking(false);

        InetSocketAddress agentAddr = new InetSocketAddress(bindingIP, portNo);
        ServerSocket serverSocket = agentSocketChannel.socket();
        serverSocket.bind(agentAddr);
        agentSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        LOG.info("Started an agent listener on {}:{} ({})", bindingIP, portNo, serverSocket.getLocalSocketAddress());
        return serverSocket.getLocalPort();
    }

    public void startServer() throws IOException {
        // keep listener running
        running = true;
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
        SocketChannelStorageAgentRequest storageAgentRequest = new SocketChannelStorageAgentRequest(channel, agentStorageProxy, storageAllocatorService, authTokenValidator, storageEventLogger);
        storageAgentRequest.initSelectionKey(channel.register(this.selector, SelectionKey.OP_READ, storageAgentRequest));
    }

    private void read(SelectionKey key) throws IOException {
        StorageAgentRequest listenerRequest = (StorageAgentRequest) key.attachment();
        listenerRequest.read();
        listenerRequest.getRequestHandler()
                .ifPresent(storageAgentRequestHandler -> storageAgentRequestHandler.handleAgentRequest());
    }

    private void write(SelectionKey key) throws IOException {
        StorageAgentRequest listenerRequest = (StorageAgentRequest) key.attachment();
        listenerRequest.getRequestHandler()
                .ifPresent(storageAgentRequestHandler -> storageAgentRequestHandler.handleAgentResponse());

    }

}
