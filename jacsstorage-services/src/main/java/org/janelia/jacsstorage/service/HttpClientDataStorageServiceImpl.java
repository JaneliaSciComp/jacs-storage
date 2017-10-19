package org.janelia.jacsstorage.service;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HttpClientDataStorageServiceImpl implements DataStorageService {

    private final String storageAgentURL;

    public HttpClientDataStorageServiceImpl(String storageAgentURL) {
        this.storageAgentURL = storageAgentURL;
    }

    @Override
    public TransferInfo persistDataStream(String dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException {
        return AgentConnectionHelper.streamDataToStorage(storageAgentURL, dataStorageFormat, dataPath, dataStream);
    }

    @Override
    public TransferInfo retrieveDataStream(String dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException {
        InputStream retrievedStream = AgentConnectionHelper.streamDataFromStorage(storageAgentURL, dataStorageFormat, dataPath);
        HashingInputStream hashingRetrievedStream = new HashingInputStream(Hashing.sha256(), retrievedStream);
        long n = ByteStreams.copy(hashingRetrievedStream, dataStream);
        return new TransferInfo(n, hashingRetrievedStream.hash().asBytes());
    }

    @Override
    public void deleteStorage(String dataPath) throws IOException {
        AgentConnectionHelper.deleteStorage(storageAgentURL, dataPath, null);
    }

    @Override
    public void cleanupStorage(String dataPath) throws IOException {
        AgentConnectionHelper.cleanupStorage(storageAgentURL, dataPath);
    }
}
