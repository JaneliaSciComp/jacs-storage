package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class StoragePathURIJsonSerializer extends JsonSerializer<OriginalStoragePathURI> {

    @Override
    public void serialize(OriginalStoragePathURI value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null || value.isEmpty()) {
            gen.writeNull();
        } else {
            gen.writeString(value.toString());
        }
    }
}
