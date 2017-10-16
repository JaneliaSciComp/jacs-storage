package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.utils.PathUtils;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

public class DataStorageServiceImpl implements DataStorageService {

    private final DataBundleIOProvider dataIOProvider;

    @Inject
    public DataStorageServiceImpl(DataBundleIOProvider dataIOProvider) {
        this.dataIOProvider = dataIOProvider;
    }

    @Override
    public TransferInfo persistDataStream(String dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataPath, dataStorageFormat);
        return bundleWriter.writeBundle(new BufferedInputStream(dataStream), dataPath);
    }

    @Override
    public TransferInfo retrieveDataStream(String dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataPath, dataStorageFormat);
        return bundleReader.readBundle(dataPath, dataStream);
    }

    @Override
    public void deleteStorage(String dataPath) throws IOException {
        PathUtils.deletePath(Paths.get(dataPath));
    }

    @Override
    public void cleanupStorage(String dataPath) throws IOException {
        PathUtils.deletePathIfEmpty(Paths.get(dataPath));
    }
}