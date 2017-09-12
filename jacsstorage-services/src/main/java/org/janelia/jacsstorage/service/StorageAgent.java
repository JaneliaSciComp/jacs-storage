package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageAgent {

    interface OperationCompleteCallback {
        void onDone();
    }

    enum StorageAgentOperation {
        PERSIST_DATA,
        RETRIEVE_DATA
    }

    enum StorageAgentState {
        IDLE,
        READ_HEADER,
        READ_DATA,
        WRITE_DATA
    }

    StorageAgentState getState();

    /**
     * Create the header buffer
     * @param op
     * @param format
     * @param location
     * @return
     * @throws IOException
     */
    byte[] getHeaderBuffer(StorageAgentOperation op, JacsStorageFormat format, String location) throws IOException;
    void readHeader(ByteBuffer buffer) throws IOException;
    int readData(ByteBuffer buffer) throws IOException;
    int writeData(ByteBuffer buffer) throws IOException;
    void endWritingData(OperationCompleteCallback cb) throws IOException;
}
