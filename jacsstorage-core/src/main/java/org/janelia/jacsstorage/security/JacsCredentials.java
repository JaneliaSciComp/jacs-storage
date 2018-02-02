package org.janelia.jacsstorage.security;

import com.nimbusds.jwt.JWTClaimsSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.security.Principal;

public class JacsCredentials implements Principal {
    private static final String UNAUTHENTICATED = "Unauthenticated";

    private String authSubject;
    private String subjectProxy;
    private String authToken;
    private JWTClaimsSet claims;

    public String getSubjectProxy() {
        return subjectProxy;
    }

    public String getAuthSubject() {
        return authSubject;
    }

    public JacsCredentials setAuthSubject(String authSubject) {
        this.authSubject = authSubject;
        return this;
    }

    public JacsCredentials setSubjectProxy(String subjectProxy) {
        this.subjectProxy = subjectProxy;
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
        if (StringUtils.isNotBlank(subjectProxy)) {
            return subjectProxy;
        } else if (StringUtils.isNotBlank(authSubject)) {
            return authSubject;
        } else {
            return UNAUTHENTICATED;
        }
    }

    public String getSubjectKey() {
        return JacsSubjectHelper.getTypeFromSubjectKey(getName()) + ":" + JacsSubjectHelper.getNameFromSubjectKey(getName());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("authSubject", authSubject)
                .append("subjectProxy", subjectProxy)
                .append("authToken", authToken)
                .append("claims", claims)
                .toString();
    }
}
