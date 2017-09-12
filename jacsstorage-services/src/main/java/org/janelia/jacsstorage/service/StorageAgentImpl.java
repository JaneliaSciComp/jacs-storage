package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
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
    public byte[] getHeaderBuffer(StorageAgentOperation op, JacsStorageFormat format, String location) throws IOException {
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
    public void readHeader(ByteBuffer buffer) throws IOException {
        if (cmdSizeValueBuffer == null) {
            cmdSizeValueBuffer = ByteBuffer.allocate(4);
            state = StorageAgentState.READ_HEADER;
            BufferUtils.copyBuffers(buffer, cmdSizeValueBuffer);
        }
        if (cmdSizeValueBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, cmdSizeValueBuffer);
        } else if (cmdBuffer == null) {
            cmdSizeValueBuffer.flip();
            int cmdSize = cmdSizeValueBuffer.getInt();
            cmdBuffer = ByteBuffer.allocate(cmdSize);
        }
        if (cmdBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, cmdBuffer);
            if (!cmdBuffer.hasRemaining()) {
                cmdBuffer.flip();
                MessageUnpacker cmdUnpacker = MessagePack.newDefaultUnpacker(cmdBuffer);
                StorageAgentOperation op = StorageAgentOperation.valueOf(cmdUnpacker.unpackString());
                JacsStorageFormat format = JacsStorageFormat.valueOf(cmdUnpacker.unpackString());
                String path = cmdUnpacker.unpackString();
                cmdUnpacker.close();
                switch (op) {
                    case PERSIST_DATA:
                        beginWritingData(format, path);
                        break;
                    case RETRIEVE_DATA:
                        beginReadingData(format, path);
                        break;
                    default:
                        throw new UnsupportedOperationException("Operation " + op + " is not supported");
                }
            }
        }
    }

    private void beginReadingData(JacsStorageFormat format, String source) throws IOException {
        state = StorageAgentState.READ_DATA;
        readerPipe = Pipe.open();
        BundleReader bundleReader = dataIOProvider.getBundleReader(format);
        OutputStream senderStream = Channels.newOutputStream(readerPipe.sink());
        agentExecutor.execute(() -> {
            try {
                bundleReader.readBundle(source, senderStream);
                readerPipe.sink().close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                state = StorageAgentState.IDLE;
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

    private void beginWritingData(JacsStorageFormat format, String target) throws IOException {
        state = StorageAgentState.WRITE_DATA;
        writerPipe = Pipe.open();
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(format);
        InputStream receiverStream = Channels.newInputStream(writerPipe.source());
        agentExecutor.execute(() -> {
            try {
                bundleWriter.writeBundle(receiverStream, target);
                writerPipe.source().close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                writerPipe = null;
                state = StorageAgentState.IDLE;
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
    public void endWritingData(OperationCompleteCallback cb) throws IOException {
        writerCompletedCallback = cb;
        // when it's done writing close the writer pipe so that the receiver on the other end of the pipe knows we are done
        writerPipe.sink().close();
    }

}
