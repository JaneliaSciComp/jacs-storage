package org.janelia.jacsstorage.dao.mongo.utils;

import com.mongodb.client.MongoClient;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;

public class RegistryHelper {

    public static CodecRegistry createCodecRegistry(ObjectMapperFactory objectMapperFactory) {
        return CodecRegistries.fromRegistries(
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec(),
                        new EnumCodec<>(JacsStorageFormat.class),
                        new EnumCodec<>(JacsStoragePermission.class),
                        new EnumCodec<>(JacsStorageType.class)
                ),
                CodecRegistries.fromProviders(new JacksonCodecProvider(objectMapperFactory))
        );
    }

}
