package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
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
import java.util.concurrent.Executor;

public class StorageAgent {

    public interface OperationCompleteCallback {
        void onDone();
    }

    enum StorageAgentOperation {
        PERSIST_DATA,
        RETRIEVE_DATA
    }

    enum StorageAgentState {
        IDLE,
        READ_ACTION,
        READ_DATA,
        WRITE_DATA
    }

    private final Executor agentExecutor;
    private final DataBundleIOProvider dataIOProvider;
    private final ByteBuffer cmdSizeValueBuffer;

    private StorageAgentState state;
    private ByteBuffer cmdBuffer;
    private Pipe writerPipe;
    private Pipe readerPipe;

    @Inject
    public StorageAgent(Executor agentExecutor, DataBundleIOProvider dataIOProvider) {
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
        cmdSizeValueBuffer = ByteBuffer.allocate(4);
        state = StorageAgentState.IDLE;
    }

    ByteBuffer getCmdSizeValueBuffer() {
        return cmdSizeValueBuffer;
    }

    ByteBuffer getCmdBuffer() {
        return cmdBuffer;
    }

    StorageAgentState getState() {
        return state;
    }

    public void beginReadingAction() throws IOException {
        state = StorageAgentState.READ_ACTION;
        cmdSizeValueBuffer.flip();
        MessageUnpacker cmdSizeUnpacker = MessagePack.newDefaultUnpacker(cmdSizeValueBuffer);
        int cmdSize = cmdSizeUnpacker.unpackInt();
        cmdSizeUnpacker.close();
        cmdBuffer = ByteBuffer.allocate(cmdSize);
    }

    public void readAction() throws IOException {
        cmdBuffer.flip();
        MessageUnpacker cmdUnpacker = MessagePack.newDefaultUnpacker(cmdBuffer);
        StorageAgentOperation op = StorageAgentOperation.valueOf(cmdUnpacker.unpackString());
        JacsStorageFormat format = JacsStorageFormat.valueOf(cmdUnpacker.unpackString());
        String path = cmdUnpacker.unpackString();
        switch (op) {
            case PERSIST_DATA:
                beginWritingData(format, path, null);
                break;
            case RETRIEVE_DATA:
                beginReadingData(format, path, null);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + op + " is not supported");
        }
    }

    public void beginWritingData(JacsStorageFormat format, String target, OperationCompleteCallback cb) throws IOException {
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
                if (cb != null) {
                    cb.onDone();
                }
            }
        });
    }

    public int writeData(byte[] buffer, int offset, int length) throws IOException {
        Pipe.SinkChannel writeChannel = writerPipe.sink();
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
        int totalBytesWritten = 0;
        while (byteBuffer.hasRemaining()) {
            int nBytes = writeChannel.write(byteBuffer);
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

    public void endWritingData() throws IOException {
        // when it's done writing close the writer pipe so that the receiver on the other end of the pipe knows we are done
        writerPipe.sink().close();
    }

    public void beginReadingData(JacsStorageFormat format, String source, OperationCompleteCallback cb) throws IOException {
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
                if (cb != null) {
                    cb.onDone();
                }
            }
        });
    }

    public int readData(byte[] buffer, int offset, int length) throws IOException {
        Pipe.SourceChannel readChannel = readerPipe.source();
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
        int totalBytesRead = 0;
        while (byteBuffer.hasRemaining()) {
            int nBytes = readChannel.read(byteBuffer);
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

}
