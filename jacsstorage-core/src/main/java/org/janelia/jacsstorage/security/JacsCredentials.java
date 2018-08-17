package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;

import java.security.Principal;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class JacsCredentials implements Principal {
    public static final String SYSTEM_APP = "system:app";
    public static final String ADMIN = "admin";

    private static final String UNAUTHENTICATED = "Unauthenticated";

    public static JacsCredentials fromTokenAndSubject(TokenCredentials tokenCredentials, String subjectName) {
        JacsCredentials credentials = new JacsCredentials();
        credentials.setAuthName(tokenCredentials.getAuthName().orElse(subjectName));
        credentials.setSubjectName(subjectName);
        credentials.setAuthToken(StringUtils.defaultIfBlank(tokenCredentials.getAuthToken(), ""));
        credentials.setClaims(tokenCredentials.getClaims());
        credentials.addRoles(tokenCredentials.getRoles());
        return credentials;
    }

    private String authName; // authenticated username
    private String subjectName; // subject principal on behalf of whom the action is performed
    private String authToken;
    private Map<String, Object> claims;
    private Set<String> roles = new LinkedHashSet<>();

    public String getAuthName() {
        return authName;
    }

    private void setAuthName(String authName) {
        this.authName = authName;
    }

    public String getSubjectName() {
        return subjectName;
    }

    private void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getAuthToken() {
        return authToken;
    }

    private void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    private void setClaims(Map<String, Object> claims) {
        this.claims = claims;
    }

    @Override
    public String getName() {
        return getSubjectKey();
    }

    private String getPrincipalSubject() {
        if (StringUtils.isNotBlank(subjectName)) {
            return subjectName;
        } else if (StringUtils.isNotBlank(authName)) {
            return authName;
        } else {
            return UNAUTHENTICATED;
        }
    }

    public String getSubjectKey() {
        return JacsSubjectHelper.nameAsSubjectKey(getPrincipalSubject());
    }

    public String getAuthKey() {
        return JacsSubjectHelper.nameAsSubjectKey(authName);
    }

    private void addRoles(Collection<String> roles) {
        this.roles.addAll(roles);
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("authName", authName)
                .append("subjectName", subjectName)
                .append("authToken", authToken)
                .append("claims", claims)
                .toString();
    }
}
