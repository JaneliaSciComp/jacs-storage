package org.janelia.jacsstorage.dao.mongo.utils;

import com.fasterxml.jackson.databind.module.SimpleModule;

import java.util.Date;

public class MongoModule extends SimpleModule {

    public MongoModule() {
        setNamingStrategy(new MongoNamingStrategy());
        addSerializer(Date.class, new ISODateSerializer());
        addDeserializer(Date.class, new ISODateDeserializer());
        addDeserializer(Long.class, new MongoNumberLongDeserializer());
        addDeserializer(Number.class, new MongoNumberBigIntegerDeserializer());
    }
}
