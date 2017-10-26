package org.janelia.jacsstorage.filter;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.security.JacsSecurityContext;
import org.janelia.jacsstorage.security.AuthTokenValidator;

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
    private static final String BEARER_PREFIX = "Bearer ";

    @Context
    private ResourceInfo resourceInfo;
    @Inject
    private AuthTokenValidator jwtTokenValidator;
    @Inject @PropertyValue(name = "JWT.SecretKey")
    private String jwtSecretKey;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Method method = resourceInfo.getResourceMethod();
        if (method.isAnnotationPresent(PermitAll.class)) {
            // everybody is allowed to access the method
            return;
        }
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        String authProperty = headers.getFirst(AUTHORIZATION_HEADER);
        String jwt = null;
        if (StringUtils.startsWithIgnoreCase(authProperty, BEARER_PREFIX)) {
            jwt = authProperty.substring(BEARER_PREFIX.length());
        }
        if (StringUtils.isBlank(jwt)) {
            requestContext.abortWith(
                    Response.status(Response.Status.FORBIDDEN)
                            .entity(ImmutableMap.of("errormessage", "Resource is not accessible without proper authentication"))
                            .build()
            );
        }
        AuthTokenValidator tokenValidator = new AuthTokenValidator(jwtSecretKey);
        JacsSecurityContext securityContext = new JacsSecurityContext(tokenValidator.validateJwtToken(jwt),
                "https".equals(requestContext.getUriInfo().getRequestUri().getScheme()),
                "JWT");
        requestContext.setSecurityContext(securityContext);
    }
}