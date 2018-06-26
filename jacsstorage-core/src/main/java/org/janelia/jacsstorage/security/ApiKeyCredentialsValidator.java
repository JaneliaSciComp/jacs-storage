package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ApiKeyCredentialsValidator implements TokenCredentialsValidator {
    private static final Logger LOG = LoggerFactory.getLogger(ApiKeyCredentialsValidator.class);

    private final String apiKey;

    public ApiKeyCredentialsValidator(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public boolean acceptToken(String token) {
        return StringUtils.isNotBlank(getTokenKey(token));
    }

    private String getTokenKey(String token) {
        String tokenKey = "";
        if (StringUtils.startsWithIgnoreCase(token, "APIKEY ")) {
            tokenKey = token.substring("APIKEY ".length()).trim();
        }
        return tokenKey;
    }

    @Override
    public Optional<JacsCredentials> validateToken(String token, String subject) {
        String tokenKey = getTokenKey(token);
        if (tokenKey.equals(apiKey)) {
            return Optional.of(new JacsCredentials()
                    .setAuthSubject(subject)
                    .setSubjectProxy(subject)
                    .setAuthToken(tokenKey)
            );
        } else {
            LOG.warn("Invalid API Key: {}", tokenKey);
            return Optional.empty();
        }
    }
}
