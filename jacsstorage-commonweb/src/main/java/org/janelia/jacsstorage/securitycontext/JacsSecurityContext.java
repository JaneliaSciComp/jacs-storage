package org.janelia.jacsstorage.securitycontext;

import java.security.Principal;

import jakarta.ws.rs.core.SecurityContext;

import org.janelia.jacsstorage.security.JacsCredentials;

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
