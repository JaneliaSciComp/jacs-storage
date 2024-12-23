package org.janelia.jacsstorage.service.n5.impl;

import com.google.gson.GsonBuilder;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

public class S3N5Reader extends N5KeyValueReader {

    public S3N5Reader(S3Adapter s3Adapter, String basePrefix) {
        super(new S3KeyValueAccess(s3Adapter, basePrefix),
                basePrefix,
                new GsonBuilder(),
                false);
    }

}
