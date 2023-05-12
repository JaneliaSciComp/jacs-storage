package org.janelia.jacsstorage.clients.api;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class StoragePathURIDeserializer extends JsonDeserializer<StoragePathURI> {

    @Override
    public StoragePathURI deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        String nodeVal = node.asText();
        return new StoragePathURI(nodeVal);
    }
}
