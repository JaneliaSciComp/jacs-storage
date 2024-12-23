package org.janelia.jacsstorage.filter;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.AggregatedTokenCredentialsValidator;
import org.janelia.jacsstorage.security.ApiKeyCredentialsValidator;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.JwtTokenCredentialsValidator;
import org.janelia.jacsstorage.security.TokenCredentialsValidator;
import org.janelia.jacsstorage.securitycontext.JacsSecurityContext;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class AuthFilter implements ContainerRequestFilter {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String SUBJECT_PARAM_NAME = "JacsSubject";
    private static final String AUTHORIZATION_COOKIE = "JacsToken";

    @Context
    private ResourceInfo resourceInfo;
    @Inject @PropertyValue(name = "JWT.SecretKey")
    private String jwtSecretKey;
    @Inject @PropertyValue(name = "StorageService.ApiKey")
    private String apiKey;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Method method = resourceInfo.getResourceMethod();
        boolean authenticationRequired = method.isAnnotationPresent(RequireAuthentication.class) ||
                method.getDeclaringClass().isAnnotationPresent(RequireAuthentication.class);
        if (!authenticationRequired) {
            // everybody is allowed to access the method
            return;
        }
        String authToken = getTokenFromRequest(requestContext);
        String subject = getSubjectFromRequest(requestContext);

        try {
            TokenCredentialsValidator tokenValidator = getTokenValidator();
            JacsSecurityContext securityContext = tokenValidator.validateToken(authToken)
                    .map(tokenCredentials -> new JacsSecurityContext(
                            JacsCredentials.fromTokenAndSubject(tokenCredentials, subject),
                            "https".equals(requestContext.getUriInfo().getRequestUri().getScheme()),
                            "JWT"))
                    .orElseThrow(() -> new SecurityException("Invalid authentication token " + authToken));
            requestContext.setSecurityContext(securityContext);
        } catch (Exception e) {
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity(new ErrorResponse(e.getMessage()))
                            .type(MediaType.APPLICATION_JSON)
                            .build()
            );
        }
    }

    private TokenCredentialsValidator getTokenValidator() {
        return new AggregatedTokenCredentialsValidator(ImmutableList.of(
                new ApiKeyCredentialsValidator(apiKey),
                new JwtTokenCredentialsValidator(jwtSecretKey)
        ));
    }

    private String getTokenFromRequest(ContainerRequestContext requestContext) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        String token = getTokenFromAuthorizationHeader(headers);
        if (StringUtils.isNotBlank(token)) {
            return token;
        } else {
            return getTokenFromCookie(requestContext);
        }
    }

    private String getSubjectFromRequest(ContainerRequestContext requestContext) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        String subject = getSubjectFromHeader(headers);
        if (StringUtils.isNotBlank(subject)) {
            return subject;
        } else {
            return getSubjectFromCookie(requestContext);
        }
    }

    private String getTokenFromAuthorizationHeader(MultivaluedMap<String, String> headers) {
        String authorizationHeader = headers.getFirst(AUTHORIZATION_HEADER);
        if (StringUtils.isNotBlank(authorizationHeader)) {
            return authorizationHeader;
        } else {
            return "";
        }
    }

    private String getTokenFromCookie(ContainerRequestContext requestContext) {
        Map<String, Cookie> requestCookies = requestContext.getCookies();
        Cookie authorizationCookie = requestCookies.get(AUTHORIZATION_COOKIE);
        if (authorizationCookie != null) {
            String authorizationToken = authorizationCookie.getValue();
            if (StringUtils.isNotBlank(authorizationToken)) {
                return AUTHORIZATION_COOKIE + " " + authorizationToken;
            } else {
                return "";
            }
        } else {
            return "";
        }
    }

    private String getSubjectFromHeader(MultivaluedMap<String, String> headers) {
        return headers.getFirst(SUBJECT_PARAM_NAME);
    }

    private String getSubjectFromCookie(ContainerRequestContext requestContext) {
        Map<String, Cookie> requestCookies = requestContext.getCookies();
        Cookie subjectCookie = requestCookies.get(SUBJECT_PARAM_NAME);
        if (subjectCookie != null) {
            return subjectCookie.getValue();
        } else {
            return "";
        }
    }

}
