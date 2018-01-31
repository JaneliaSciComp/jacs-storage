package org.janelia.jacsstorage.datatransfer.impl;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsDataLocation;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.janelia.jacsstorage.datatransfer.State;
import org.janelia.jacsstorage.datatransfer.StorageMessageHeader;
import org.janelia.jacsstorage.datatransfer.TransferState;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutorService;

public class DataTransferServiceImpl implements DataTransferService {
    private final Logger LOG = LoggerFactory.getLogger(DataTransferServiceImpl.class);

    private final ExecutorService backgroundTransferExecutor;
    private final DataBundleIOProvider dataIOProvider;

    @Inject
    public DataTransferServiceImpl(ExecutorService backgroundTransferExecutor, DataBundleIOProvider dataIOProvider) {
        this.backgroundTransferExecutor = backgroundTransferExecutor;
        this.dataIOProvider = dataIOProvider;
    }

    @Override
    public void beginDataTransfer(TransferState<StorageMessageHeader> transferState) throws IOException {
        StorageMessageHeader messageHeader = transferState.getMessageType();
        switch (messageHeader.getOperation()) {
            case PERSIST_DATA:
                beginWritingData(new JacsDataLocation(messageHeader.getLocationOrDefault(), messageHeader.getFormat()), transferState);
                break;
            case RETRIEVE_DATA:
                beginReadingData(new JacsDataLocation(messageHeader.getLocationOrDefault(), messageHeader.getFormat()), transferState);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + messageHeader.getOperation() + " is not supported");
        }
    }

    private void beginReadingData(JacsDataLocation dataLocation, TransferState<?> transferState) throws IOException {
        LOG.info("Begin reading data from: {}", dataLocation);
        BundleReader bundleReader;
        try {
            bundleReader = dataIOProvider.getBundleReader(dataLocation.getStorageFormat());
        } catch (Exception e) {
            transferState.setErrorMessage("Error opening the reader for " + dataLocation + " - " + e.getMessage());
            transferState.setState(State.READ_DATA_ERROR);
            return;
        }
        transferState.openDataTransferChannel();
        OutputStream senderStream = transferState.getDataWriteChannel()
                .map((WritableByteChannel dataWriteChannel) -> {
                    transferState.setState(State.READ_DATA_STARTED);
                    return Channels.newOutputStream(dataWriteChannel);
                })
                .orElseThrow(() -> {
                    transferState.setErrorMessage("Transfer channel for reading" + dataLocation + " has not been opened");
                    transferState.setState(State.READ_DATA_ERROR);
                    return new IllegalStateException("Data transfer channel has not been opened for reading " + dataLocation);
                });
        backgroundTransferExecutor.execute(() -> {
            try {
                transferState.setState(State.READ_DATA);
                TransferInfo ti = bundleReader.readBundle(dataLocation.getPath(), senderStream);
                transferState.setTransferredBytes(ti.getNumBytes());
                transferState.setChecksum(ti.getChecksum());
                transferState.setState(State.READ_DATA_COMPLETE);
            } catch (Exception e) {
                LOG.error("Error while reading {}", dataLocation, e);
                transferState.setErrorMessage("Error reading data: " + e.getMessage());
                transferState.setState(State.READ_DATA_ERROR);
            } finally {
                transferState.getDataWriteChannel().ifPresent(dataWriteChannel -> closeQuietly(dataWriteChannel));
            }
        });
    }

    @Override
    public int readData(ByteBuffer buffer, TransferState<?> transferState) throws IOException {
        return transferState.getDataReadChannel()
                .map((ReadableByteChannel readChannel) -> {
                    int totalBytesRead = 0;
                    while (buffer.hasRemaining()) {
                        int nBytes;
                        try {
                            nBytes = readChannel.read(buffer);
                        } catch (IOException e) {
                            LOG.error("Error transfering {} from the read channel", transferState, e);
                            nBytes = -1;
                        }
                        if (nBytes < 0) {
                            if (totalBytesRead == 0) {
                                totalBytesRead = -1;
                                closeQuietly(readChannel);
                                transferState.closeDataTransferChannel();
                            }
                            break;
                        }
                        totalBytesRead += nBytes;
                    }
                    return totalBytesRead;
                })
                .orElse(-1);
    }

    private void beginWritingData(JacsDataLocation dataLocation, TransferState<?> transferState) throws IOException {
        LOG.info("Begin writing data to: {}", dataLocation);
        BundleWriter bundleWriter;
        try {
            bundleWriter = dataIOProvider.getBundleWriter(dataLocation.getStorageFormat());
        } catch (Exception e) {
            transferState.setErrorMessage("Error opening the writer for " + dataLocation + " - " + e.getMessage());
            transferState.setState(State.WRITE_DATA_ERROR);
            return;
        }
        transferState.openDataTransferChannel();
        InputStream receiverStream = transferState.getDataReadChannel()
                .map((ReadableByteChannel dataReadChannel) -> {
                    transferState.setState(State.WRITE_DATA_STARTED);
                    return Channels.newInputStream(dataReadChannel);
                })
                .orElseThrow(() -> {
                    LOG.warn("Transfer channel for writing {} has not been opened", dataLocation);
                    transferState.setErrorMessage("Transfer channel for writing" + dataLocation + " has not been opened");
                    transferState.setState(State.WRITE_DATA_ERROR);
                    return new IllegalStateException("Data transfer channel has not been opened for writing " + dataLocation);
                });
        backgroundTransferExecutor.execute(() -> {
            try {
                transferState.setState(State.WRITE_DATA);
                TransferInfo ti = bundleWriter.writeBundle(receiverStream, dataLocation.getPath());
                transferState.setTransferredBytes(ti.getNumBytes());
                transferState.setPersistedBytes(PathUtils.getSize(dataLocation.getPath()));
                transferState.setChecksum(ti.getChecksum());
                transferState.setState(State.WRITE_DATA_COMPLETE);
            } catch (Exception e) {
                LOG.error("Error while writing {}", dataLocation, e);
                transferState.setErrorMessage("Error writing data: " + e.getMessage());
                transferState.setState(State.WRITE_DATA_ERROR);
            } finally {
                transferState.getDataReadChannel().ifPresent(dataReadChannel -> closeQuietly(dataReadChannel));
                transferState.closeDataTransferChannel();
            }
        });
    }

    @Override
    public int writeData(ByteBuffer buffer, TransferState<?> transferState) throws IOException {
        return transferState.getDataWriteChannel()
               .map((WritableByteChannel writeChannel) -> {
                   int totalBytesWritten = 0;
                   while (buffer.hasRemaining()) {
                       int nBytes;
                       try {
                           nBytes = writeChannel.write(buffer);
                       } catch (Exception e) {
                           LOG.error("Error transfering {} to the write channel", transferState, e);
                           nBytes = -1;
                       }
                       if (nBytes < 0) {
                           if (totalBytesWritten == 0) {
                               totalBytesWritten = -1;
                           }
                           break;
                       }
                       totalBytesWritten += nBytes;
                   }
                   return totalBytesWritten;
               })
               .orElse(-1);
    }

    @Override
    public void endDataTransfer(TransferState<?> transferState) throws IOException {
        transferState.getDataWriteChannel().ifPresent(dataWriteChannel -> closeQuietly(dataWriteChannel));
    }

    private void closeQuietly(Closeable c) {
        try {
            c.close();
        } catch (IOException ignore) {
            // ignore
            LOG.trace("Error closing {}", c, ignore);
        }
    }
}
