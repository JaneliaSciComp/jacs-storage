module org.janelia.jacsstorage.services {
    requires cdi.api;
    requires commons.collections4;
//    requires guava;
//    requires jersey.client;
//    requires logback.classic;
//    requires logback.core;
    requires bson;
    requires mongodb.driver.core;
//    requires org.apache.commons.lang3;
    requires org.janelia.jacsstorage.api;
    requires org.janelia.jacsstorage.core;
    requires reflections;
//    requires slf4j.api;

    exports org.janelia.jacsstorage.dao;
    exports org.janelia.jacsstorage.service;

}
