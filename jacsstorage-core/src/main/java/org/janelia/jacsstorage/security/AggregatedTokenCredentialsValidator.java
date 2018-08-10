package org.janelia.jacsstorage.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class AggregatedTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatedTokenCredentialsValidator.class);

    private final List<TokenCredentialsValidator> tokenValidatorsList;

    public AggregatedTokenCredentialsValidator(List<TokenCredentialsValidator> tokenValidatorsList) {
        this.tokenValidatorsList = tokenValidatorsList;
    }

    @Override
    public boolean acceptToken(String token) {
        for (TokenCredentialsValidator tokenValidator : tokenValidatorsList) {
            if (tokenValidator.acceptToken(token)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<TokenCredentials> validateToken(String token) {
        for (TokenCredentialsValidator tokenValidator : tokenValidatorsList) {
            if (tokenValidator.acceptToken(token)) {
                return tokenValidator.validateToken(token);
            }
        }
        LOG.warn("No token validator found for: {}", token);
        return Optional.empty();
    }
}
