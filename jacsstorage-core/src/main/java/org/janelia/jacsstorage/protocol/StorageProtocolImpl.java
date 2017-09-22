package org.janelia.jacsstorage.protocol;

import org.apache.commons.compress.utils.IOUtils;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsDataLocation;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.utils.BufferUtils;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.util.concurrent.ExecutorService;

public class StorageProtocolImpl implements StorageProtocol {
    private final Logger LOG = LoggerFactory.getLogger(StorageProtocolImpl.class);

    private final ExecutorService backgroundTransferExecutor;
    private final DataBundleIOProvider dataIOProvider;

    private State state;
    private String errormessage;
    private ByteBuffer requestSizeValueBuffer;
    private ByteBuffer requestBuffer;
    private ByteBuffer responseSizeValueBuffer;
    private ByteBuffer responseBuffer;
    private Pipe writerPipe;
    private Pipe readerPipe;

    @Inject
    public StorageProtocolImpl(ExecutorService backgroundTransferExecutor, DataBundleIOProvider dataIOProvider) {
        this.backgroundTransferExecutor = backgroundTransferExecutor;
        this.dataIOProvider = dataIOProvider;
        state = State.IDLE;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public String getLastErrorMessage() {
        return errormessage;
    }

    @Override
    public byte[] encodeRequest(StorageMessageHeader request) throws IOException {
        MessageBufferPacker requestPacker = MessagePack.newDefaultBufferPacker();
        requestPacker.packString(request.getOperation().name())
                .packString(request.getFormat().name())
                .packString(request.getLocation());
        requestPacker.close();
        byte[] msgBytes = requestPacker.toByteArray();
        byte[] requestBuffer = new byte[4 + msgBytes.length];
        ByteBuffer buffer = ByteBuffer.wrap(requestBuffer);
        buffer.putInt(msgBytes.length);
        buffer.put(msgBytes);
        return requestBuffer;
    }

    @Override
    public byte[] encodeResponse(StorageMessageResponse response) throws IOException {
        MessageBufferPacker responsePacker = MessagePack.newDefaultBufferPacker();
        responsePacker
                .packInt(response.getStatus())
                .packString(response.getMessage())
                .packLong(response.getSize())
                ;
        responsePacker.close();
        byte[] msgBytes = responsePacker.toByteArray();
        byte[] responseBuffer = new byte[4 + msgBytes.length];
        ByteBuffer buffer = ByteBuffer.wrap(responseBuffer);
        buffer.putInt(msgBytes.length);
        buffer.put(msgBytes);
        return responseBuffer;
    }

    @Override
    public boolean readRequest(ByteBuffer buffer, Holder<StorageMessageHeader> requestHolder) throws IOException {
        if (requestSizeValueBuffer == null) {
            requestSizeValueBuffer = ByteBuffer.allocate(4);
            state = State.READ_REQUEST;
        }
        if (requestSizeValueBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, requestSizeValueBuffer);
            if (requestSizeValueBuffer.hasRemaining()) {
                return false;
            }
        }
        if (requestBuffer == null) {
            requestSizeValueBuffer.flip();
            int requestSize = requestSizeValueBuffer.getInt();
            requestBuffer = ByteBuffer.allocate(requestSize);
        }
        if (requestBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, requestBuffer);
            if (!requestBuffer.hasRemaining()) {
                requestBuffer.flip();
                MessageUnpacker requestUnpacker = MessagePack.newDefaultUnpacker(requestBuffer);
                Operation op = Operation.valueOf(requestUnpacker.unpackString());
                JacsStorageFormat format = JacsStorageFormat.valueOf(requestUnpacker.unpackString());
                String path = requestUnpacker.unpackString();
                requestUnpacker.close();
                requestHolder.setData(new StorageMessageHeader(op, format, path));
                return true;
            } else {
                return false;
            }
        } else {
            // nothing left to read from the requestBuffer so it must've finished it before
            return true;
        }
    }

    @Override
    public boolean readResponse(ByteBuffer buffer, Holder<StorageMessageResponse> responseHolder) throws IOException {
        if (responseSizeValueBuffer == null) {
            responseSizeValueBuffer = ByteBuffer.allocate(4);
        }
        if (responseSizeValueBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, responseSizeValueBuffer);
            if (responseSizeValueBuffer.hasRemaining()) {
                return false;
            }
        }
        if (responseBuffer == null) {
            responseSizeValueBuffer.flip();
            int responseSize = responseSizeValueBuffer.getInt();
            responseBuffer = ByteBuffer.allocate(responseSize);
        }
        if (responseBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, responseBuffer);
            if (!responseBuffer.hasRemaining()) {
                responseBuffer.flip();
                MessageUnpacker responseUnpacker = MessagePack.newDefaultUnpacker(responseBuffer);
                int status = responseUnpacker.unpackInt();
                String message = responseUnpacker.unpackString();
                long size = responseUnpacker.unpackLong();
                responseHolder.setData(new StorageMessageResponse(status, message, size));
                return true;
            } else {
                return false;
            }
        } else {
            // nothing left to read from the responseBuffer so it must've finished it before
            return true;
        }
    }

    @Override
    public void beginDataTransfer(StorageMessageHeader request) throws IOException {
        switch (request.getOperation()) {
            case PERSIST_DATA:
                beginWritingData(new JacsDataLocation(request.getLocation(), request.getFormat()));
                break;
            case RETRIEVE_DATA:
                beginReadingData(new JacsDataLocation(request.getLocation(), request.getFormat()));
                break;
            default:
                throw new UnsupportedOperationException("Operation " + request.getOperation() + " is not supported");
        }
    }

    private void beginReadingData(JacsDataLocation dataLocation) throws IOException {
        LOG.info("Begin reading data from: {}", dataLocation);
        state = State.READ_DATA_STARTED;
        readerPipe = Pipe.open();
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataLocation.getStorageFormat());
        OutputStream senderStream = Channels.newOutputStream(readerPipe.sink());
        backgroundTransferExecutor.execute(() -> {
            try {
                state = State.READ_DATA;
                bundleReader.readBundle(dataLocation.getPath(), senderStream);
                state = State.READ_DATA_COMPLETE;
            } catch (Exception e) {
                LOG.error("Error while reading {}", dataLocation, e);
                state = State.READ_DATA_ERROR;
                errormessage = "Error reading data: " + e.getMessage();
            } finally {
                IOUtils.closeQuietly(readerPipe.sink());
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
        LOG.info("Begin writing data to: {}", dataLocation);
        state = State.WRITE_DATA_STARTED;
        writerPipe = Pipe.open();
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataLocation.getStorageFormat());
        InputStream receiverStream = Channels.newInputStream(writerPipe.source());
        backgroundTransferExecutor.execute(() -> {
            try {
                state = State.WRITE_DATA;
                bundleWriter.writeBundle(receiverStream, dataLocation.getPath());
                state = State.WRITE_DATA_COMPLETE;
            } catch (Exception e) {
                LOG.error("Error while writing {}", dataLocation, e);
                state = State.WRITE_DATA_ERROR;
                errormessage = "Error writing data: " + e.getMessage();
            } finally {
                IOUtils.closeQuietly(writerPipe.source());
                writerPipe = null;
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
    public void endDataTransfer() throws IOException {
        if (writerPipe != null) {
            // when it's done writing close the writer pipe so that the receiver on the other end of the pipe knows we are done
            writerPipe.sink().close();
        }
    }

}
