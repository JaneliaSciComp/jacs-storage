package org.janelia.jacsstorage.utils;

import java.net.InetAddress;

public class NetUtils {
    public static String getCurrentHostName() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostName();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
