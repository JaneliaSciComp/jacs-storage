package org.janelia.jacsstorage.security;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AggregatedTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatedTokenCredentialsValidator.class);

    private final List<TokenCredentialsValidator> tokenValidatorsList;

    public AggregatedTokenCredentialsValidator(List<TokenCredentialsValidator> tokenValidatorsList) {
        this.tokenValidatorsList = tokenValidatorsList;
    }

    @Override
    public String authorizationScheme() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JacsCredentials validateToken(String token, String subject) {
        if (StringUtils.isBlank(token)) {
            throw new SecurityException("Authentication token is empty");
        }
        for (TokenCredentialsValidator tokenValidator : tokenValidatorsList) {
            String tokenValidatorAuthScheme = tokenValidator.authorizationScheme() + " ";
            if (StringUtils.startsWithIgnoreCase(token, tokenValidatorAuthScheme)) {
                return tokenValidator.validateToken(token.substring(tokenValidatorAuthScheme.length()), subject);
            }
        }
        LOG.warn("No token validator found for: {}", token);
        throw new SecurityException("Unsupported authorization scheme for token " + token);
    }
}
