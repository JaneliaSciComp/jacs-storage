package org.janelia.jacsstorage.dao.mongo.utils;

import com.mongodb.MongoClient;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry(ObjectMapperFactory objectMapperFactory) {
        return CodecRegistries.fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec(),
                        new EnumCodec<>(JacsStorageFormat.class),
                        new EnumCodec<>(JacsStoragePermission.class)
                ),
                CodecRegistries.fromProviders(new JacksonCodecProvider(objectMapperFactory))
        );
    }

}
