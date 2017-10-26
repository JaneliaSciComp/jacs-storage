package org.janelia.jacsstorage.security;

import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.commons.lang3.StringUtils;

import java.security.Principal;

public class JacsCredentials implements Principal {
    public static final String UNAUTHENTICATED = "Unauthenticated";

    private String subject;
    private String authToken;
    private JWTClaimsSet claims;

    public String getSubject() {
        return getName();
    }

    public JacsCredentials setSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public String getAuthToken() {
        return authToken == null ? "" : authToken;
    }

    public JacsCredentials setAuthToken(String authToken) {
        this.authToken = authToken;
        return this;
    }

    public JWTClaimsSet getClaims() {
        return claims;
    }

    public JacsCredentials setClaims(JWTClaimsSet claims) {
        this.claims = claims;
        return this;
    }

    @Override
    public String getName() {
        return StringUtils.isBlank(subject) ? UNAUTHENTICATED : subject;
    }

}
