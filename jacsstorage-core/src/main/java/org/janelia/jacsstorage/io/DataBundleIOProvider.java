package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class DataBundleIOProvider {

    private final Instance<BundleReader> bundleReaderSource;
    private final Instance<BundleWriter> bundleWriterSource;

    @Inject
    public DataBundleIOProvider(Instance<BundleReader> bundleReaderSource, Instance<BundleWriter> bundleWriterSource) {
        this.bundleReaderSource = bundleReaderSource;
        this.bundleWriterSource = bundleWriterSource;
    }

    public BundleReader getBundleReader(JacsStorageFormat format) {
        for (BundleReader bundleReader : getSupportedReaders(bundleReaderSource)) {
            if (bundleReader.getSupportedFormats().contains(format)) {
                return bundleReader;
            }
        }
        throw new IllegalArgumentException("Unsuported data bundle read format: " + format);
    }

    public BundleWriter getBundleWriter(JacsStorageFormat format) {
        for (BundleWriter bundleWriter : getSupportedWriters(bundleWriterSource)) {
            if (bundleWriter.getSupportedFormats().contains(format)) {
                return bundleWriter;
            }
        }
        throw new IllegalArgumentException("Unsuported data bundle write format: " + format);
    }

    private List<BundleReader> getSupportedReaders(Instance<BundleReader> bundleReaders) {
        List<BundleReader> supportedReaders = new ArrayList<>();
        for (BundleReader bundleReader : bundleReaders) {
            supportedReaders.add(bundleReader);
        }
        return supportedReaders;
    }

    private List<BundleWriter> getSupportedWriters(Instance<BundleWriter> bundleWriters) {
        List<BundleWriter> supportedWriters = new ArrayList<>();
        for (BundleWriter bundleWriter : bundleWriters) {
            supportedWriters.add(bundleWriter);
        }
        return supportedWriters;
    }
}
