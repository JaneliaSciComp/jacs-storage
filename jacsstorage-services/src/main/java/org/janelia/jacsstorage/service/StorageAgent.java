package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class StorageAgent {

    private final Executor agentExecutor;
    private final DataBundleIOProvider dataIOProvider;

    private Pipe writerPipe;
    private Pipe readerPipe;
    private Semaphore doneWriting = new Semaphore(1);

    @Inject
    public StorageAgent(Executor agentExecutor, DataBundleIOProvider dataIOProvider) {
        this.agentExecutor = agentExecutor;
        this.dataIOProvider = dataIOProvider;
    }

    public void beginReadingData(JacsStorageFormat format, String source) throws IOException {
        readerPipe = Pipe.open();
        BundleReader bundleReader = dataIOProvider.getBundleReader(format);
        OutputStream senderStream = Channels.newOutputStream(readerPipe.sink());
        agentExecutor.execute(() -> {
            bundleReader.readBundle(source, senderStream);
            try {
                senderStream.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void beginWritingData(JacsStorageFormat format, String target) throws IOException {
        try {
            doneWriting.acquire();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
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
                doneWriting.release();
            }
        });
    }

    public void endWritingData() throws IOException {
        // when it's done writing close the writer pipe so that the receiver on the other end of the pipe knows we are done
        writerPipe.sink().close();
        try {
            doneWriting.acquire();
        } catch (InterruptedException e) {
        } finally {
            doneWriting.release();
        }
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

}
