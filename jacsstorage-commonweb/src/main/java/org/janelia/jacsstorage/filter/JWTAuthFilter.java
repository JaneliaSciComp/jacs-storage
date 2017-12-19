package org.janelia.jacsstorage.filter;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.AggregatedTokenCredentialsValidator;
import org.janelia.jacsstorage.security.ApiKeyCredentialsValidator;
import org.janelia.jacsstorage.security.JwtTokenCredentialsValidator;
import org.janelia.jacsstorage.security.CachedTokenCredentialsValidator;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.JacsSecurityContext;
import org.janelia.jacsstorage.security.TokenCredentialsValidator;

import javax.annotation.Priority;
import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Method;

@Priority(Priorities.AUTHENTICATION)
public class JWTAuthFilter implements ContainerRequestFilter {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String SUBJECT_HEADER = "JacsSubject";

    @Context
    private ResourceInfo resourceInfo;
    @Inject @PropertyValue(name = "JWT.SecretKey")
    private String jwtSecretKey;
    @Inject @PropertyValue(name = "StorageService.ApiKey")
    private String apiKey;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Method method = resourceInfo.getResourceMethod();
        if (method.isAnnotationPresent(PermitAll.class)) {
            // everybody is allowed to access the method
            return;
        }
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        String authHeaderValue = headers.getFirst(AUTHORIZATION_HEADER);
        String subject = headers.getFirst(SUBJECT_HEADER);
        TokenCredentialsValidator tokenValidator = getTokenValidator();
        try {
            JacsSecurityContext securityContext = new JacsSecurityContext(tokenValidator.validateToken(authHeaderValue, subject),
                    "https".equals(requestContext.getUriInfo().getRequestUri().getScheme()),
                    "JWT");
            requestContext.setSecurityContext(securityContext);
        } catch (Exception e) {
            requestContext.abortWith(
                    Response.status(Response.Status.FORBIDDEN)
                            .entity(new ErrorResponse(e.getMessage()))
                            .build()
            );
        }
    }

    private TokenCredentialsValidator getTokenValidator() {
        return new AggregatedTokenCredentialsValidator(ImmutableList.of(
                new ApiKeyCredentialsValidator(apiKey),
                new CachedTokenCredentialsValidator(new JwtTokenCredentialsValidator(jwtSecretKey))
        ));
    }
}