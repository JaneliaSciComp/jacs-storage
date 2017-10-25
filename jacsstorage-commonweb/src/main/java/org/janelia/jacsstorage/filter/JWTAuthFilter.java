package org.janelia.jacsstorage.filter;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Priority;
import javax.annotation.security.PermitAll;
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

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Method method = resourceInfo.getResourceMethod();
        if (method.isAnnotationPresent(PermitAll.class)) {
            // everybody is allowed to access the method
            return;
        }
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        String authProperty = headers.getFirst(AUTHORIZATION_HEADER);
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!! AUTH PROPERTY: " + authProperty);
        String token = null;
        if (StringUtils.startsWithIgnoreCase(authProperty, BEARER_PREFIX)) {
            token = authProperty.substring(BEARER_PREFIX.length());
        }
        if (StringUtils.isBlank(token)) {
            requestContext.abortWith(
                    Response.status(Response.Status.FORBIDDEN)
                            .entity(ImmutableMap.of("errormessage", "Resource is not accessible without proper authentication"))
                            .build()
            );
        }
    }
}