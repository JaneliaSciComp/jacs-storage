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
        READ_REQUEST,
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
     * Encode the request
     * @param request
     * @return
     * @throws IOException
     */
    byte[] encodeRequest(StorageMessageHeader request) throws IOException;
    /**
     * Encode the response
     * @param response
     * @return
     * @throws IOException
     */
    byte[] encodeResponse(StorageMessageResponse response) throws IOException;
    /**
     * Consume the request from the buffer and return a value that specifies if the entire request has been consumed or not.
     * @param buffer to read from.
     * @param requestHolder request read
     * @return true if it finished reading the request false otherwise
     * @throws IOException
     */
    boolean readRequest(ByteBuffer buffer, Holder<StorageMessageHeader> requestHolder) throws IOException;
    /**
     * Consume the response buffer
     * @param buffer
     * @param responseHolder request read
     * @return true if it finished reading the response false otherwise
     * @throws IOException
     */
    boolean readResponse(ByteBuffer buffer, Holder<StorageMessageResponse> responseHolder) throws IOException;
    /**
     * Initiate the data transfer.
     * @throws IOException
     */
    void beginDataTransfer(StorageMessageHeader request) throws IOException;
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
