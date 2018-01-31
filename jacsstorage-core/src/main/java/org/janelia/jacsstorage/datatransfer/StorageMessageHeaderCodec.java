package org.janelia.jacsstorage.datatransfer;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class StorageMessageHeaderCodec implements MessageDataCodec<StorageMessageHeader> {

    @Override
    public ByteBuffer encodeMessage(StorageMessageHeader data) throws IOException {
        MessageBufferPacker messageHeaderPacker = MessagePack.newDefaultBufferPacker();
        BigInteger dataBundleId;
        if (data.getDataBundleId() != null) {
            dataBundleId = new BigInteger(data.getDataBundleId().toString());
        } else {
            dataBundleId = new BigInteger("0");
        }
        messageHeaderPacker
                .packBigInteger(dataBundleId)
                .packString(data.getAuthToken())
                .packString(data.getOperation().name())
                .packString(data.getFormat() == null ? "" : data.getFormat().name())
                .packString(data.getLocationOrDefault())
                .packString(data.getMessageOrDefault());
        messageHeaderPacker.close();
        byte[] msgBytes = messageHeaderPacker.toByteArray();
        ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
        return msgBuffer;
    }

    @Override
    public StorageMessageHeader decodeMessage(ByteBuffer buffer) throws IOException {
        MessageUnpacker messageHeaderUnpacker = MessagePack.newDefaultUnpacker(buffer);
        BigInteger dataBundleId = messageHeaderUnpacker.unpackBigInteger();
        String authToken = messageHeaderUnpacker.unpackString();
        DataTransferService.Operation op = DataTransferService.Operation.valueOf(messageHeaderUnpacker.unpackString());
        String formatString = messageHeaderUnpacker.unpackString();
        JacsStorageFormat format = StringUtils.isBlank(formatString) ? null : JacsStorageFormat.valueOf(formatString);
        String path = messageHeaderUnpacker.unpackString();
        String message = messageHeaderUnpacker.unpackString();
        messageHeaderUnpacker.close();
        return new StorageMessageHeader(dataBundleId, authToken, op, format, path, message);
    }
}
