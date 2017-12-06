package org.janelia.jacsstorage.security;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthTokenValidator {
    private static final String USERNAME_CLAIM = "user_name";
    private static final Logger LOG = LoggerFactory.getLogger(AuthTokenValidator.class);

    private final String secretKey;

    public AuthTokenValidator(String secretKey) {
        this.secretKey = secretKey;
    }

    public JacsCredentials validateJwtToken(String jwt) {
        try {
            ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
            JWSAlgorithm expectedJWSAlg = JWSAlgorithm.HS256;
            JWKSource<SecurityContext> jwtKeySource = new ImmutableSecret<>(secretKey.getBytes());
            JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(expectedJWSAlg, jwtKeySource);
            jwtProcessor.setJWSKeySelector(keySelector);

            JWTClaimsSet claimsSet = jwtProcessor.process(jwt, null);

            return new JacsCredentials()
                    .setSubject(StringUtils.defaultIfBlank(claimsSet.getSubject(), claimsSet.getStringClaim(USERNAME_CLAIM)))
                    .setAuthToken(jwt)
                    .setClaims(claimsSet);
        } catch (Exception e) {
            LOG.error("Error while validating {}", jwt, e);
            throw new SecurityException(e);
        }
    }
}
