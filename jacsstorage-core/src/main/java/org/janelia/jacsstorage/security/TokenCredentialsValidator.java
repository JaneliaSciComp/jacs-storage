package org.janelia.jacsstorage.security;

public interface TokenCredentialsValidator {
    String authorizationScheme();
    JacsCredentials validateToken(String token, String subject);
}
