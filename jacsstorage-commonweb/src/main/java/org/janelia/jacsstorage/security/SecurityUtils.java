package org.janelia.jacsstorage.security;

import javax.ws.rs.core.SecurityContext;

public class SecurityUtils {
    public static JacsCredentials getUserPrincipal(SecurityContext securityContext) {
        if (securityContext != null) {
            return (JacsCredentials) securityContext.getUserPrincipal();
        } else {
            return new JacsCredentials();
        }
    }
}
