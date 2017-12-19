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

public class JwtTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final String USERNAME_CLAIM = "user_name";
    private static final Logger LOG = LoggerFactory.getLogger(JwtTokenCredentialsValidator.class);

    private final String secretKey;

    public JwtTokenCredentialsValidator(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public String authorizationScheme() {
        return "Bearer";
    }

    public JacsCredentials validateToken(String token, String subject) {
        if (StringUtils.isBlank(token)) {
            throw new SecurityException("Authentication token is empty");
        }
        try {
            ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
            JWSAlgorithm expectedJWSAlg = JWSAlgorithm.HS256;
            JWKSource<SecurityContext> jwtKeySource = new ImmutableSecret<>(secretKey.getBytes());
            JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(expectedJWSAlg, jwtKeySource);
            jwtProcessor.setJWSKeySelector(keySelector);

            JWTClaimsSet claimsSet = jwtProcessor.process(token, null);

            return new JacsCredentials()
                    .setAuthSubject(StringUtils.defaultIfBlank(claimsSet.getSubject(), claimsSet.getStringClaim(USERNAME_CLAIM)))
                    .setSubjectProxy(subject)
                    .setAuthToken(token)
                    .setClaims(claimsSet);
        } catch (Exception e) {
            LOG.error("Error while validating {}", token, e);
            throw new SecurityException(e);
        }
    }
}
