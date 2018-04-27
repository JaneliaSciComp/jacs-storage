package org.janelia.jacsstorage.io;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;

import java.io.BufferedInputStream;
import java.io.InputStream;

public abstract class AbstractBundleWriter implements BundleWriter {

    @TimedMethod(
            argList = {1},
            logResult = true
    )
    public TransferInfo writeBundle(InputStream stream, String target) {
        try {
            HashingInputStream hashingInputStream = new HashingInputStream(Hashing.sha256(), stream);
            long nBytes = writeBundleBytes(new BufferedInputStream(hashingInputStream), target);
            return new TransferInfo(nBytes, hashingInputStream.hash().asBytes());
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected abstract long writeBundleBytes(InputStream stream, String target) throws Exception;
}
