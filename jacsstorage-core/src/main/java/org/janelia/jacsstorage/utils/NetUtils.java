package org.janelia.jacsstorage.utils;

import java.net.InetAddress;

public class NetUtils {
    public static String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
