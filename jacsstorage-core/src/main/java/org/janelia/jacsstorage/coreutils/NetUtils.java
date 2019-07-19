package org.janelia.jacsstorage.coreutils;

import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;

public class NetUtils {
    private static final char DEFAULT_HOST_PORT_SEPARATOR = ':';

    private static volatile String hostname;

    public static String getCurrentHostName() {
        if (StringUtils.isBlank(hostname)) {
            try {
                InetAddress ip = InetAddress.getLocalHost();
                hostname = ip.getHostName();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return hostname;
    }

    public static String createStorageHostId(String hostname, String portValue) {
        return createStorageHostId(hostname, portValue, null); // use default separator
    }

    public static String createStorageHostId(String hostname, String portValue, String separator) {
        StringBuilder builder = new StringBuilder(hostname);
        if (StringUtils.isNotBlank(portValue)) {
            if (separator == null) {
                builder.append(DEFAULT_HOST_PORT_SEPARATOR);
            } else {
                builder.append(separator);
            }
            builder.append(portValue);
        }
        return builder.toString();
    }
}
