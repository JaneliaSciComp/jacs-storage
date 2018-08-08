package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class StoragePathURIJsonDeserializer extends JsonDeserializer<StoragePathURI> {
    @Override
    public StoragePathURI deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String storagePath = p.getText();
        return new StoragePathURI(storagePath);
    }
}
