package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.utils.PathUtils;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DataStorageServiceImpl implements DataStorageService {

    private final DataBundleIOProvider dataIOProvider;

    @Inject
    public DataStorageServiceImpl(DataBundleIOProvider dataIOProvider) {
        this.dataIOProvider = dataIOProvider;
    }

    @Override
    public TransferInfo persistDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.writeBundle(new BufferedInputStream(dataStream), dataPath.toString());
    }

    @Override
    public TransferInfo retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.readBundle(dataPath.toString(), dataStream);
    }

    @Override
    public List<DataNodeInfo> listDataEntries(Path dataPath, JacsStorageFormat dataStorageFormat, int depth) {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.listBundleContent(dataPath.toString(), depth);
    }

    @Override
    public long createDirectoryEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat) {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.createDirectoryEntry(dataPath.toString(), entryName);
    }

    @Override
    public long createFileEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream contentStream) {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.createFileEntry(dataPath.toString(), entryName, contentStream);
    }

    @Override
    public long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, OutputStream outputStream) throws IOException {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.readDataEntry(dataPath.toString(), entryName, outputStream);
    }

    @Override
    public void deleteStorage(Path dataPath) throws IOException {
        PathUtils.deletePath(dataPath);
    }

    @Override
    public void cleanupStoragePath(Path dataPath) throws IOException {
        PathUtils.deletePathIfEmpty(dataPath);
    }
}
