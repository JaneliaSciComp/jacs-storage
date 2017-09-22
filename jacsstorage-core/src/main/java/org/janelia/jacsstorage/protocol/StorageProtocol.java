package org.janelia.jacsstorage.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageProtocol {

    enum Operation {
        PERSIST_DATA,
        RETRIEVE_DATA,
        PROCESS_RESPONSE,
        PROCESS_ERROR
    }

    enum State {
        IDLE,
        READ_MESSAGE_HEADER,
        READ_DATA_STARTED,
        WRITE_DATA_STARTED,
        READ_DATA,
        WRITE_DATA,
        READ_DATA_COMPLETE,
        READ_DATA_ERROR,
        WRITE_DATA_COMPLETE,
        WRITE_DATA_ERROR
    }

    class Holder<D> {
        private D data;

        public Holder() {
            this.data = data;
        }

        public Holder(D data) {
            this.data = data;
        }

        public D getData() {
            return data;
        }

        public void setData(D data) {
            this.data = data;
        }
    }

    /**
     * Get current state.
     * @return
     */
    State getState();

    /**
     * Get last error message.
     * @return
     */
    String getLastErrorMessage();
    /**
     * Encode the message header.
     * @param messageHeader
     * @return
     * @throws IOException
     */
    byte[] encodeMessageHeader(StorageMessageHeader messageHeader) throws IOException;
    /**
     * Encode the response
     * @param messageResponse
     * @return
     * @throws IOException
     */
    byte[] encodeMessageResponse(StorageMessageResponse messageResponse) throws IOException;
    /**
     * Consume the message header from the buffer and return a value that specifies if the entire message header has been consumed or not.
     * @param buffer to read from.
     * @param messageHeaderHolder holder for the read message header
     * @return true if it finished reading the header false otherwise
     * @throws IOException
     */
    boolean readMessageHeader(ByteBuffer buffer, Holder<StorageMessageHeader> messageHeaderHolder) throws IOException;
    /**
     * Consume the response buffer
     * @param buffer
     * @param messageResponseHolder holder for the read response
     * @return true if it finished reading the response false otherwise
     * @throws IOException
     */
    boolean readMessageResponse(ByteBuffer buffer, Holder<StorageMessageResponse> messageResponseHolder) throws IOException;
    /**
     * Initiate the data transfer.
     * @throws IOException
     */
    void beginDataTransfer(StorageMessageHeader messageHeader) throws IOException;
    /**
     * Terminate the data transfer.
     * @throws IOException
     */
    void endDataTransfer() throws IOException;
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
}
