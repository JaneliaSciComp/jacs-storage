package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.utils.BufferUtils;

import javax.enterprise.inject.Vetoed;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

/**
 * @param <T> message type that is being transferred
 */
@Vetoed
public class TransferState<T> {

    private volatile State state;
    private long transferredBytes;
    private long persistedBytes;
    private String errorMessage;
    private ByteBuffer messageTypeSizeBuffer;
    private ByteBuffer messageTypeBuffer;
    private byte[] checksum;
    private T messageType;
    private volatile Pipe dataTransferPipe;

    public boolean hasReadEntireMessageType() {
        return messageType != null;
    }

    public boolean readMessageType(ByteBuffer buffer, MessageDataCodec<T> messageTypeCodec) throws IOException {
        if (messageTypeSizeBuffer == null) {
            messageTypeSizeBuffer = ByteBuffer.allocate(4);
            state = State.READ_MESSAGE_HEADER;
        }
        if (messageTypeSizeBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, messageTypeSizeBuffer);
            if (messageTypeSizeBuffer.hasRemaining()) {
                // if it could not read the entire message type size return
                return false;
            }
        }
        if (messageTypeBuffer == null) {
            messageTypeSizeBuffer.flip();
            int requestSize = messageTypeSizeBuffer.getInt();
            messageTypeBuffer = ByteBuffer.allocate(requestSize);
        }
        if (messageTypeBuffer.hasRemaining()) {
            BufferUtils.copyBuffers(buffer, messageTypeBuffer);
            if (!messageTypeBuffer.hasRemaining()) {
                messageTypeBuffer.flip();
                messageType = messageTypeCodec.decodeMessage(messageTypeBuffer);
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    public byte[] writeMessageType(T t, MessageDataCodec<T> messageTypeCodec) throws IOException {
        ByteBuffer newMessageTypeBuffer = messageTypeCodec.encodeMessage(t);
        byte[] newMessageTypeBytes = new byte[4 + newMessageTypeBuffer.remaining()];
        ByteBuffer wrappedNewMessageTypeBytes = ByteBuffer.wrap(newMessageTypeBytes);
        wrappedNewMessageTypeBytes.putInt(newMessageTypeBuffer.remaining());
        BufferUtils.copyBuffers(newMessageTypeBuffer, wrappedNewMessageTypeBytes);
        return newMessageTypeBytes;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public long getTransferredBytes() {
        return transferredBytes;
    }

    public void setTransferredBytes(long transferredBytes) {
        this.transferredBytes = transferredBytes;
    }

    public long getPersistedBytes() {
        return persistedBytes;
    }

    public void setPersistedBytes(long persistedBytes) {
        this.persistedBytes = persistedBytes;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public T getMessageType() {
        return messageType;
    }

    public TransferState<T> setMessageType(T messageType) {
        this.messageType = messageType;
        return this;
    }

    public byte[] getChecksum() {
        return checksum;
    }

    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }

    public void openDataTransferChannel() throws IOException {
        dataTransferPipe = Pipe.open();
    }

    public void closeDataTransferChannel() {
        dataTransferPipe = null;
    }

    public Optional<WritableByteChannel> getDataWriteChannel() {
        return dataTransferPipe != null ? Optional.of(dataTransferPipe.sink()) : Optional.empty();
    }

    public Optional<ReadableByteChannel> getDataReadChannel() {
        return dataTransferPipe != null ? Optional.of(dataTransferPipe.source()) : Optional.empty();
    }
}
