package org.janelia.jacsstorage.security;

import java.util.Optional;

public interface TokenCredentialsValidator {
    boolean acceptToken(String token);
    Optional<TokenCredentials> validateToken(String token);
}
