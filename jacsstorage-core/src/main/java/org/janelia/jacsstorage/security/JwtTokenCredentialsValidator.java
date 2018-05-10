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

import java.util.Optional;

public class JwtTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final String USERNAME_CLAIM = "user_name";
    private static final Logger LOG = LoggerFactory.getLogger(JwtTokenCredentialsValidator.class);

    private final String secretKey;

    public JwtTokenCredentialsValidator(String secretKey) {
        this.secretKey = secretKey;
    }

    @Override
    public boolean acceptToken(String token) {
        return StringUtils.isNotBlank(getJwt(token));
    }

    private String getJwt(String token) {
        String jwt = "";
        if (StringUtils.startsWithIgnoreCase(token, "Bearer ")) {
            jwt = token.substring("Bearer ".length()).trim();
        } else if (StringUtils.startsWithIgnoreCase(token, "JacsToken ")) {
            jwt = token.substring("JacsToken ".length()).trim();
        }
        return jwt;
    }

    public Optional<JacsCredentials> validateToken(String token, String subject) {
        String jwt = getJwt(token);
        try {
            ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
            JWSAlgorithm expectedJWSAlg = JWSAlgorithm.HS256;
            JWKSource<SecurityContext> jwtKeySource = new ImmutableSecret<>(secretKey.getBytes());
            JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(expectedJWSAlg, jwtKeySource);
            jwtProcessor.setJWSKeySelector(keySelector);

            JWTClaimsSet claimsSet = jwtProcessor.process(jwt, null);

            return Optional.of(new JacsCredentials()
                    .setAuthSubject(StringUtils.defaultIfBlank(claimsSet.getSubject(), claimsSet.getStringClaim(USERNAME_CLAIM)))
                    .setSubjectProxy(subject)
                    .setAuthToken(jwt)
                    .setClaims(claimsSet));
        } catch (Exception e) {
            LOG.error("Error while validating {}", jwt, e);
            throw new SecurityException(e);
        }
    }
}
