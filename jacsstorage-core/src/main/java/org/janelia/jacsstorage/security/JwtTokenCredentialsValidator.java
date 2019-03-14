package org.janelia.jacsstorage.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.nio.charset.Charset;
import java.util.Optional;

public class JwtTokenCredentialsValidator implements TokenCredentialsValidator {
    private static final String USERNAME_CLAIM = "user_name";
    private static final Logger LOG = LoggerFactory.getLogger(JwtTokenCredentialsValidator.class);

    private final byte[] secretKeyBytes;

    public JwtTokenCredentialsValidator(String secretKey) {
        this.secretKeyBytes = StringUtils.isNotBlank(secretKey) ? secretKey.getBytes(Charset.forName("UTF-8")) : new byte[32];
    }

    @Override
    public boolean acceptToken(String token) {
        return StringUtils.isNotBlank(getJwt(token));
    }

    private String getJwt(String token) {
        String jwt;
        if (StringUtils.startsWithIgnoreCase(token, "Bearer ")) {
            jwt = token.substring("Bearer ".length()).trim();
        } else if (StringUtils.startsWithIgnoreCase(token, "JacsToken ")) {
            jwt = token.substring("JacsToken ".length()).trim();
        } else {
            jwt = "";
        }
        return jwt;
    }

    public Optional<TokenCredentials> validateToken(String token) {
        String jwt = getJwt(token);
        try {
            SecretKey key = Keys.hmacShaKeyFor(secretKeyBytes);
            Claims claimsSet = Jwts.parser()
                    .setSigningKey(key).parseClaimsJws(jwt).getBody();
            return Optional.of(new TokenCredentials()
                    .setAuthName(StringUtils.defaultIfBlank(claimsSet.getSubject(), claimsSet.get(USERNAME_CLAIM, String.class)))
                    .setAuthToken(jwt)
                    .setClaims(claimsSet)
            );
        } catch (Exception e) {
            LOG.error("Error while validating {}", jwt, e);
            throw new SecurityException(e);
        }
    }
}
