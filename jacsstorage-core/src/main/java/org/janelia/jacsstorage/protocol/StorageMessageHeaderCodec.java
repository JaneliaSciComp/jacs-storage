package org.janelia.jacsstorage.protocol;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StorageMessageHeaderCodec implements MessageDataCodec<StorageMessageHeader> {

    @Override
    public ByteBuffer encodeMessage(StorageMessageHeader data) throws IOException {
        MessageBufferPacker messageHeaderPacker = MessagePack.newDefaultBufferPacker();
        messageHeaderPacker.packString(data.getOperation().name())
                .packString(data.getFormat().name())
                .packString(data.getLocationOrDefault())
                .packString(data.getMessageOrDefault());
        messageHeaderPacker.close();
        byte[] msgBytes = messageHeaderPacker.toByteArray();
        ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
        return msgBuffer;
    }

    @Override
    public StorageMessageHeader decodeMessage(ByteBuffer buffer) throws IOException {
        MessageUnpacker requestUnpacker = MessagePack.newDefaultUnpacker(buffer);
        StorageService.Operation op = StorageService.Operation.valueOf(requestUnpacker.unpackString());
        JacsStorageFormat format = JacsStorageFormat.valueOf(requestUnpacker.unpackString());
        String path = requestUnpacker.unpackString();
        String message = requestUnpacker.unpackString();
        requestUnpacker.close();
        return new StorageMessageHeader(op, format, path, message);
    }
}
