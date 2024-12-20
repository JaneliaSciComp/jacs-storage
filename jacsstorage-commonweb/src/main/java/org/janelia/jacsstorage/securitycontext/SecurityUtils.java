package org.janelia.jacsstorage.securitycontext;

import jakarta.ws.rs.core.SecurityContext;

import org.janelia.jacsstorage.security.JacsCredentials;

public class SecurityUtils {
    public static JacsCredentials getUserPrincipal(SecurityContext securityContext) {
        if (securityContext != null) {
            return (JacsCredentials) securityContext.getUserPrincipal();
        } else {
            return new JacsCredentials();
        }
    }
}
