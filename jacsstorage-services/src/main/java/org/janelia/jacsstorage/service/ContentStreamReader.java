package org.janelia.jacsstorage.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.commons.io.input.TeeInputStream;

public interface ContentStreamReader {
    long streamContentTo(String contentLocation, OutputStream outputStream);

    default InputStream streamContent(String contentLocation) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamContentTo(contentLocation, outputStream);
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

}
