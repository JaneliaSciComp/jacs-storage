package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public interface StorageProtocol {

    interface OperationCompleteCallback {
        void onDone();
    }

    enum Operation {
        PERSIST_DATA,
        RETRIEVE_DATA
    }

    enum State {
        IDLE,
        READ_HEADER,
        READ_DATA,
        WRITE_DATA,
        DATA_TRANSFER_COMPLETED,
        DATA_TRANSFER_ERROR
    }

    State getState();

    /**
     * Create the header buffer
     * @param op
     * @param format
     * @param location
     * @return
     * @throws IOException
     */
    byte[] createHeader(Operation op, JacsStorageFormat format, String location) throws IOException;
    /**
     * Consume the header from the buffer and return a value that specifies if the header has been consumed or not.
     * @param buffer to read from.
     * @param nextState if the header has just been consumed and the nextState is specified it updates the state.
     * @return 1 if the header has just been consumed; 0 if the header has not been consumed yet; -1 if the header has been consumed in a previous call.
     * @throws IOException
     */
    int readHeader(ByteBuffer buffer, Optional<State> nextState) throws IOException;
    /**
     * Initiate the data transfer.
     * @throws IOException
     */
    void beginDataTransfer() throws IOException;

    /**
     * Read data from the buffer.
     * @param buffer
     * @return
     * @throws IOException
     */
    int readData(ByteBuffer buffer) throws IOException;

    /**
     * Write data to the buffer.
     * @param buffer
     * @return
     * @throws IOException
     */
    int writeData(ByteBuffer buffer) throws IOException;

    /**
     * Terminate the data transfer. The operation may be an asynchronous operation so it provides a notification callback to be executed when the transfer is effectively
     * done.
     * @param cb
     * @throws IOException
     */
    void terminateDataTransfer(OperationCompleteCallback cb) throws IOException;
}
