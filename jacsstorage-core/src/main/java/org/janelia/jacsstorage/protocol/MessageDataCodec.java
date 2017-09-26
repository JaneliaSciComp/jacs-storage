package org.janelia.jacsstorage.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface MessageDataCodec<D> {

    ByteBuffer encodeMessage(D data) throws IOException;
    D decodeMessage(ByteBuffer buffer) throws IOException;
}
