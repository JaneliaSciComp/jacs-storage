package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiKeyCredentialsValidator implements TokenCredentialsValidator {
    private static final Logger LOG = LoggerFactory.getLogger(ApiKeyCredentialsValidator.class);

    private final String apiKey;

    public ApiKeyCredentialsValidator(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public String authorizationScheme() {
        return "APIKEY";
    }

    @Override
    public JacsCredentials validateToken(String token, String subject) {
        if (StringUtils.isBlank(token)) {
            throw new SecurityException("Authentication token is empty");
        }
        if (token.equals(apiKey)) {
            return new JacsCredentials()
                    .setAuthSubject(subject)
                    .setSubjectProxy(subject)
                    .setAuthToken(token);
        } else {
            LOG.warn("Invalid API Key: {}", token);
            throw new SecurityException("Invalid API Key");
        }
    }
}
