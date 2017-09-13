package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsDataLocation;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.utils.BufferUtils;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class StorageAgentImpl implements StorageAgent {

    private final ExecutorService agentExecutor;
    private final DataBundleIOProvider dataIOProvider;

    private StorageAgentState state;
    private ByteBuffer cmdSizeValueBuffer;
    private ByteBuffer cmdBuffer;
    private Pipe writerPipe;
    private Pipe readerPipe;
    private OperationCompleteCallback writerCompletedCallback;

    @Inject
    public StorageAgentImpl(ExecutorService agentExecutor, DataBundleIOProvider dataIOProvider) {
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
        state = StorageAgentState.IDLE;
    }

    @Override
    public StorageAgentState getState() {
        return state;
    }

    @Override
    public byte[] createHeader(StorageAgentOperation op, JacsStorageFormat format, String location) throws IOException {
        MessageBufferPacker cmdPacker = MessagePack.newDefaultBufferPacker();
        cmdPacker.packString(op.name())
                .packString(format.name())
                .packString(location);
        cmdPacker.close();
        byte[] msgBytes = cmdPacker.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(4 + msgBytes.length);
        buffer.putInt(msgBytes.length);
        buffer.put(msgBytes);
        buffer.flip();
        byte[] headerBuffer = new byte[buffer.remaining()];
        buffer.get(headerBuffer);
        return headerBuffer;
    }

    @Override
    public int readHeader(ByteBuffer buffer, Optional<StorageAgentState> nextState) throws IOException {
        if (cmdSizeValueBuffer == null) {
            cmdSizeValueBuffer = ByteBuffer.allocate(4);
            state = StorageAgentState.READ_HEADER;
        }
        if (cmdSizeValueBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, cmdSizeValueBuffer);
            if (cmdSizeValueBuffer.hasRemaining()) {
                return 0;
            }
        }
        if (cmdBuffer == null) {
            cmdSizeValueBuffer.flip();
            int cmdSize = cmdSizeValueBuffer.getInt();
            cmdBuffer = ByteBuffer.allocate(cmdSize);
        }
        if (cmdBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, cmdBuffer);
            if (!cmdBuffer.hasRemaining()) {
                nextState.map(s -> state = s);
                return 1;
            } else {
                return 0;
            }
        } else {
            return -1;
        }
    }

    @Override
    public void beginDataTransfer() throws IOException {
        if (cmdBuffer != null && !cmdBuffer.hasRemaining()) {
            cmdBuffer.flip();
            MessageUnpacker cmdUnpacker = MessagePack.newDefaultUnpacker(cmdBuffer);
            StorageAgentOperation op = StorageAgentOperation.valueOf(cmdUnpacker.unpackString());
            JacsStorageFormat format = JacsStorageFormat.valueOf(cmdUnpacker.unpackString());
            String path = cmdUnpacker.unpackString();
            cmdUnpacker.close();
            switch (op) {
                case PERSIST_DATA:
                    beginWritingData(new JacsDataLocation(path, format));
                    break;
                case RETRIEVE_DATA:
                    beginReadingData(new JacsDataLocation(path, format));
                    break;
                default:
                    throw new UnsupportedOperationException("Operation " + op + " is not supported");
            }
        }
    }

    private void beginReadingData(JacsDataLocation dataLocation) throws IOException {
        state = StorageAgentState.READ_DATA;
        readerPipe = Pipe.open();
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataLocation.getStorageFormat());
        OutputStream senderStream = Channels.newOutputStream(readerPipe.sink());
        agentExecutor.execute(() -> {
            try {
                bundleReader.readBundle(dataLocation.getPath(), senderStream);
                state = StorageAgentState.DATA_TRANSFER_COMPLETED;
                readerPipe.sink().close();
            } catch (IOException e) {
                state = StorageAgentState.DATA_TRANSFER_ERROR;
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public int readData(ByteBuffer buffer) throws IOException {
        Pipe.SourceChannel readChannel = readerPipe.source();
        int totalBytesRead = 0;
        while (buffer.hasRemaining()) {
            int nBytes = readChannel.read(buffer);
            if (nBytes < 0) {
                if (totalBytesRead == 0) {
                    totalBytesRead = -1;
                    readChannel.close();
                    readerPipe = null;
                }
                break;
            }
            totalBytesRead += nBytes;
        }
        return totalBytesRead;
    }

    private void beginWritingData(JacsDataLocation dataLocation) throws IOException {
        state = StorageAgentState.WRITE_DATA;
        writerPipe = Pipe.open();
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataLocation.getStorageFormat());
        InputStream receiverStream = Channels.newInputStream(writerPipe.source());
        agentExecutor.execute(() -> {
            try {
                bundleWriter.writeBundle(receiverStream, dataLocation.getPath());
                state = StorageAgentState.DATA_TRANSFER_COMPLETED;
                writerPipe.source().close();
            } catch (IOException e) {
                state = StorageAgentState.DATA_TRANSFER_ERROR;
                throw new UncheckedIOException(e);
            } finally {
                writerPipe = null;
                if (writerCompletedCallback != null) {
                    writerCompletedCallback.onDone();
                }
            }
        });
    }

    @Override
    public int writeData(ByteBuffer buffer) throws IOException {
        Pipe.SinkChannel writeChannel = writerPipe.sink();
        int totalBytesWritten = 0;
        while (buffer.hasRemaining()) {
            int nBytes = writeChannel.write(buffer);
            if (nBytes < 0) {
                if (totalBytesWritten == 0) {
                    totalBytesWritten = -1;
                }
                break;
            }
            totalBytesWritten += nBytes;
        }
        return totalBytesWritten;
    }

    @Override
    public void terminateDataTransfer(OperationCompleteCallback cb) throws IOException {
        writerCompletedCallback = cb;
        // when it's done writing close the writer pipe so that the receiver on the other end of the pipe knows we are done
        writerPipe.sink().close();
    }

}
