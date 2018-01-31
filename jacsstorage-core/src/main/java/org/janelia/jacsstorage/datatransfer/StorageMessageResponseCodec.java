package org.janelia.jacsstorage.datatransfer;

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
        ;
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
        return new StorageMessageResponse(status, message);
    }
}
