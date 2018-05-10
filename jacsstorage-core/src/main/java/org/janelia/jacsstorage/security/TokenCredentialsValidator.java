package org.janelia.jacsstorage.security;

import java.util.Optional;

public interface TokenCredentialsValidator {
    boolean acceptToken(String token);
    Optional<JacsCredentials> validateToken(String token, String subject);
}
