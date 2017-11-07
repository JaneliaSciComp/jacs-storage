package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetAddress;

public class DaoStorageEventLoggerImpl implements StorageEventLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DaoStorageEventLoggerImpl.class);

    private final JacsStorageEventDao storageEventDao;

    @Inject
    public DaoStorageEventLoggerImpl(JacsStorageEventDao storageEventDao) {
        this.storageEventDao = storageEventDao;
    }

    @Override
    public JacsStorageEvent logStorageEvent(String name, String description, Object data) {
        JacsStorageEvent jacsStorageEvent = new JacsStorageEventBuilder()
                .eventName(name)
                .eventDescription(description)
                .eventHost(getLocalIP())
                .eventData(data)
                .build();
        storageEventDao.save(jacsStorageEvent);
        return jacsStorageEvent;
    }

    private String getLocalIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            LOG.error("Error getting local IP", e);
            return null;
        }
    }
}
