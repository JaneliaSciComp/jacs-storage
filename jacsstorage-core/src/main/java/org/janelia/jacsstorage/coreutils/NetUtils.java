package org.janelia.jacsstorage.coreutils;

import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;

public class NetUtils {
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
}
