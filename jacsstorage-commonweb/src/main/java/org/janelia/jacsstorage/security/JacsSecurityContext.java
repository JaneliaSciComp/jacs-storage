package org.janelia.jacsstorage.security;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;

public class JacsSecurityContext implements SecurityContext {

    private final JacsCredentials credentials;
    private final boolean secure;
    private final String authScheme;

    public JacsSecurityContext(JacsCredentials credentials, boolean secure, String authScheme) {
        this.credentials = credentials;
        this.secure = secure;
        this.authScheme = authScheme;
    }

    @Override
    public Principal getUserPrincipal() {
        return credentials;
    }

    @Override
    public boolean isUserInRole(String role) {
        return credentials.hasRole(role);
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public String getAuthenticationScheme() {
        return authScheme;
    }
}
