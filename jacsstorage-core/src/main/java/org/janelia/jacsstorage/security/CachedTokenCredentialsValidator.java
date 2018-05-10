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

import java.util.Optional;
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
    public boolean acceptToken(String token) {
        return impl.acceptToken(token);
    }

    @Override
    public Optional<JacsCredentials> validateToken(String token, String subject) {
        JacsCredentials credentials = CREDENTIALS_CACHE.getIfPresent(token);
        if (credentials == null) {
            return impl.validateToken(token, subject)
                    .map(jc -> {
                        CREDENTIALS_CACHE.put(token, jc);
                        return jc;
                    });
        } else {
            return Optional.of(credentials);
        }
    }
}
