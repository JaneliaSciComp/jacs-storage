package org.janelia.jacsstorage.security;

import com.nimbusds.jwt.JWTClaimsSet;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;

public class JacsSecurityContext implements SecurityContext {

    public static final String ADMIN = "admin";

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
        return false;
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