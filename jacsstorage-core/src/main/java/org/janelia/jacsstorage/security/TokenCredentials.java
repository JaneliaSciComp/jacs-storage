package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;
import java.util.Optional;

public class TokenCredentials {
    private String authName;
    private boolean supportsAuthName;
    private String authToken;
    private Map<String, Object> claims;

    Optional<String> getAuthName() {
        return supportsAuthName ? Optional.of(authName) : Optional.empty();
    }

    TokenCredentials setAuthName(String authName) {
        this.authName = authName;
        this.supportsAuthName = true;
        return this;
    }

    String getAuthToken() {
        return authToken;
    }

    TokenCredentials setAuthToken(String authToken) {
        this.authToken = authToken;
        return this;
    }

    Map<String, Object> getClaims() {
        return claims;
    }

    TokenCredentials setClaims(Map<String, Object> claims) {
        this.claims = claims;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("authName", authName)
                .append("authToken", authToken)
                .toString();
    }
}
