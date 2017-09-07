package org.janelia.jacsstorage.service;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;

import java.io.OutputStream;

public abstract class AbstractBundleReader implements BundleReader {

    @Override
    public TransferInfo readBundle(String source, OutputStream stream) {
        try {
            HashingOutputStream hashingOutputStream = new HashingOutputStream(Hashing.sha256(), stream);
            long nBytes = readBundleBytes(source, hashingOutputStream);
            return new TransferInfo(nBytes, hashingOutputStream.hash().asBytes());
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected abstract long readBundleBytes(String source, OutputStream stream) throws Exception;
}
