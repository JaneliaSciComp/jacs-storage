package org.janelia.jacsstorage.service;

import org.apache.commons.compress.utils.IOUtils;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsDataLocation;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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
            case PING:
                break;
            default:
                throw new UnsupportedOperationException("Operation " + messageHeader.getOperation() + " is not supported");
        }
    }

    private void beginReadingData(JacsDataLocation dataLocation, TransferState<?> transferState) throws IOException {
        LOG.info("Begin reading data from: {}", dataLocation);
        transferState.openDataTransferChannel();
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataLocation.getStorageFormat());
        OutputStream senderStream = Channels.newOutputStream(transferState.getDataWriteChannel());
        transferState.setState(State.READ_DATA_STARTED);
        backgroundTransferExecutor.execute(() -> {
            try {
                transferState.setState(State.READ_DATA);
                TransferInfo ti = bundleReader.readBundle(dataLocation.getPath(), senderStream);
                transferState.setState(State.READ_DATA_COMPLETE);
                transferState.setTransferredBytes(ti.getNumBytes());
            } catch (Exception e) {
                LOG.error("Error while reading {}", dataLocation, e);
                transferState.setErrorMessage("Error reading data: " + e.getMessage());
                transferState.setState(State.READ_DATA_ERROR);
            } finally {
                IOUtils.closeQuietly(transferState.getDataWriteChannel());
            }
        });
    }

    @Override
    public int readData(ByteBuffer buffer, TransferState<?> transferState) throws IOException {
        ReadableByteChannel readChannel = transferState.getDataReadChannel();
        int totalBytesRead = 0;
        while (buffer.hasRemaining()) {
            int nBytes = readChannel.read(buffer);
            if (nBytes < 0) {
                if (totalBytesRead == 0) {
                    totalBytesRead = -1;
                    readChannel.close();
                    transferState.closeDataTransferChannel();
                }
                break;
            }
            totalBytesRead += nBytes;
        }
        return totalBytesRead;
    }

    private void beginWritingData(JacsDataLocation dataLocation, TransferState<?> transferState) throws IOException {
        LOG.info("Begin writing data to: {}", dataLocation);
        transferState.openDataTransferChannel();
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataLocation.getStorageFormat());
        InputStream receiverStream = Channels.newInputStream(transferState.getDataReadChannel());
        transferState.setState(State.WRITE_DATA_STARTED);
        backgroundTransferExecutor.execute(() -> {
            try {
                transferState.setState(State.WRITE_DATA);
                TransferInfo ti = bundleWriter.writeBundle(receiverStream, dataLocation.getPath());
                transferState.setTransferredBytes(ti.getNumBytes());
                transferState.setPersistedBytes(PathUtils.getSize(dataLocation.getPath()));
                transferState.setState(State.WRITE_DATA_COMPLETE);
            } catch (Exception e) {
                LOG.error("Error while writing {}", dataLocation, e);
                transferState.setErrorMessage("Error writing data: " + e.getMessage());
                transferState.setState(State.WRITE_DATA_ERROR);
            } finally {
                IOUtils.closeQuietly(transferState.getDataReadChannel());
                transferState.closeDataTransferChannel();
            }
        });
    }

    @Override
    public int writeData(ByteBuffer buffer, TransferState<?> transferState) throws IOException {
        if (!transferState.isDataTransferChannelOpen()) {
            return -1;
        }
        WritableByteChannel writeChannel = transferState.getDataWriteChannel();
        int totalBytesWritten = 0;
        while (buffer.hasRemaining()) {
            int nBytes = writeChannel.write(buffer);
            if (nBytes < 0) {
                if (totalBytesWritten == 0) {
                    totalBytesWritten = -1;
                }
                break;
            }
            totalBytesWritten += nBytes;
        }
        return totalBytesWritten;
    }

    @Override
    public void endDataTransfer(TransferState<?> transferState) throws IOException {
        if (transferState.isDataTransferChannelOpen()) {
            transferState.getDataWriteChannel().close();
        }
    }

}
