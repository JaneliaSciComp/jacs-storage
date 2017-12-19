package org.janelia.jacsstorage.security;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

public class CachedTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final Cache<String, JacsCredentials> CREDENTIALS_CACHE = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(300, TimeUnit.SECONDS)
            .build();

    private final TokenCredentialsValidator impl;

    public CachedTokenCredentialsValidator(TokenCredentialsValidator impl) {
        this.impl = impl;
    }

    @Override
    public String authorizationScheme() {
        return impl.authorizationScheme();
    }

    @Override
    public JacsCredentials validateToken(String token, String subject) {
        try {
            return CREDENTIALS_CACHE.get(token, () -> impl.validateToken(token, subject));
        } catch (Exception e) {
            throw new SecurityException(e);
        }
    }
}
