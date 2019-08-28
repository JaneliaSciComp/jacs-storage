package org.janelia.jacsstorage.service.impl;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.coreutils.PathUtils;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DataStorageServiceImpl implements DataStorageService {

    private final DataBundleIOProvider dataIOProvider;

    @Inject
    public DataStorageServiceImpl(DataBundleIOProvider dataIOProvider) {
        this.dataIOProvider = dataIOProvider;
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long persistDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.writeBundle(new BufferedInputStream(dataStream), dataPath.toString());
    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, ContentFilterParams filterParams, OutputStream dataStream) throws IOException {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.readBundle(dataPath.toString(), filterParams, dataStream);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Map<String, Object> getDataEntryInfo(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat) {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.getContentInfo(dataPath.toString(), entryName);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Stream<DataNodeInfo> streamDataEntries(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, int depth) {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.streamBundleContent(dataPath.toString(), entryName, depth);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long createDirectoryEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat) {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.createDirectoryEntry(dataPath.toString(), entryName);
    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long writeDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream contentStream) {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.createFileEntry(dataPath.toString(), entryName, contentStream);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long estimateDataEntrySize(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, ContentFilterParams filterParams) {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.estimateDataEntrySize(dataPath.toString(), entryName, filterParams);
    }

    @TimedMethod(
            argList = {0, 1, 2, 3},
            logResult = true
    )
    @Override
    public long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, ContentFilterParams filterParams, OutputStream outputStream) throws IOException {
        BundleReader bundleReader = dataIOProvider.getBundleReader(dataStorageFormat);
        return bundleReader.readDataEntry(dataPath.toString(), entryName, filterParams, outputStream);
    }

    @Override
    public long deleteStorageEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat) {
        BundleWriter bundleWriter = dataIOProvider.getBundleWriter(dataStorageFormat);
        return bundleWriter.deleteEntry(dataPath.toString(), entryName);
    }

    @TimedMethod
    @Override
    public void deleteStoragePath(Path dataPath) throws IOException {
        PathUtils.deletePath(dataPath);
    }

    @TimedMethod
    @Override
    public void cleanupStoragePath(Path dataPath) throws IOException {
        PathUtils.deletePathIfEmpty(dataPath);
    }
}
