package org.janelia.jacsstorage.service;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageService {

    enum Operation {
        PING,
        PERSIST_DATA,
        RETRIEVE_DATA,
        PROCESS_RESPONSE,
        PROCESS_ERROR
    }

    /**
     * Initiate the data transfer.
     * @param transferState
     * @return
     * @throws IOException
     */
    void beginDataTransfer(TransferState<StorageMessageHeader> transferState) throws IOException;
    /**
     * Terminate the data transfer.
     * @param transferState
     * @throws IOException
     */
    void endDataTransfer(TransferState<?> transferState) throws IOException;
    /**
     * Read data from the buffer.
     * @param buffer
     * @param transferState
     * @return
     * @throws IOException
     */
    int readData(ByteBuffer buffer, TransferState<?> transferState) throws IOException;
    /**
     * Write data to the buffer.
     * @param buffer
     * @param transferState
     * @return
     * @throws IOException
     */
    int writeData(ByteBuffer buffer, TransferState<?> transferState) throws IOException;
}
