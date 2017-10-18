package org.janelia.jacsstorage.service;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StorageMessageResponseCodec implements MessageDataCodec<StorageMessageResponse> {

    @Override
    public ByteBuffer encodeMessage(StorageMessageResponse data) throws IOException {
        MessageBufferPacker responsePacker = MessagePack.newDefaultBufferPacker();
        responsePacker
                .packInt(data.getStatus())
                .packString(data.getMessage())
                .packLong(data.getTransferredBytes())
                .packLong(data.getPersistedBytes())
        ;
        responsePacker.packBinaryHeader(data.getChecksum().length);
        responsePacker.writePayload(data.getChecksum());
        responsePacker.close();
        byte[] msgBytes = responsePacker.toByteArray();
        ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
        return msgBuffer;
    }

    @Override
    public StorageMessageResponse decodeMessage(ByteBuffer buffer) throws IOException {
        MessageUnpacker responseUnpacker = MessagePack.newDefaultUnpacker(buffer);
        int status = responseUnpacker.unpackInt();
        String message = responseUnpacker.unpackString();
        long transferredBytes = responseUnpacker.unpackLong();
        long persistedBytes = responseUnpacker.unpackLong();
        int checksumLength = responseUnpacker.unpackBinaryHeader();
        byte[] checksum = new byte[checksumLength];
        responseUnpacker.readPayload(checksum);
        return new StorageMessageResponse(status, message, transferredBytes, persistedBytes, checksum);
    }
}
