module org.janelia.jacsstorage.core {
    requires cdi.api;
    requires java.activation;
    requires commons.collections4;
    requires guava;
    requires logback.core;
    requires msgpack.core;
    requires org.apache.commons.lang3;
    requires org.apache.commons.compress;
    requires org.janelia.jacsstorage.api;
    requires slf4j.api;

    exports org.janelia.jacsstorage.client;
    exports org.janelia.jacsstorage.io;
    exports org.janelia.jacsstorage.resilience;
    exports org.janelia.jacsstorage.security;
    exports org.janelia.jacsstorage.datatransfer;
    exports org.janelia.jacsstorage.utils;
}
