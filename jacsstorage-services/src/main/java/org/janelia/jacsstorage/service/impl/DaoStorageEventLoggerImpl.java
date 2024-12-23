package org.janelia.jacsstorage.service.impl;

import java.net.InetAddress;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEventBuilder;
import org.janelia.jacsstorage.service.StorageEventLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public class DaoStorageEventLoggerImpl implements StorageEventLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DaoStorageEventLoggerImpl.class);

    private final JacsStorageEventDao storageEventDao;

    @Inject
    public DaoStorageEventLoggerImpl(JacsStorageEventDao storageEventDao) {
        this.storageEventDao = storageEventDao;
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public JacsStorageEvent logStorageEvent(String name, String description, Object data, String status) {
        JacsStorageEvent jacsStorageEvent = new JacsStorageEventBuilder()
                .eventName(name)
                .eventDescription(description)
                .eventHost(getLocalIP())
                .eventData(data)
                .eventStatus(status)
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
