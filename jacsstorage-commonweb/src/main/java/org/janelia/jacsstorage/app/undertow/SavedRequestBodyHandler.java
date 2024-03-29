package org.janelia.jacsstorage.app.undertow;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.AttachmentKey;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SavedRequestBodyHandler implements HttpHandler {
    static final AttachmentKey<List<RequestBodyPart>> SAVED_REQUEST_BODY = AttachmentKey.create(List.class);

    private static final Logger LOG = LoggerFactory.getLogger(SavedRequestBodyHandler.class);

    private final HttpHandler next;
    private final boolean enabled;
    private final Set<String> supportedMethods;
    private final Set<String> supportedMimeTypes;

    SavedRequestBodyHandler(HttpHandler next, boolean enabled) {
        this.next = next;
        this.enabled = enabled;
        this.supportedMethods = ImmutableSet.of("PUT", "POST", "PROPFIND", "MKCOL");
        this.supportedMimeTypes = ImmutableSet.of("application/json", "application/xml", "multipart/");
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (enabled && supportedMethods.contains(exchange.getRequestMethod().toString())) {
            String mimeType = exchange.getRequestHeaders().getFirst(Headers.CONTENT_TYPE);
            if (mimeType != null && supportedMimeTypes.stream().filter(supportedMimeType -> mimeType.startsWith(supportedMimeType)).findAny().orElse(null) != null) {
                boolean isMultipart;
                String boundary;
                if (mimeType.startsWith("multipart/")) {
                    isMultipart = true;
                    boundary = Headers.extractQuotedValueFromHeader(mimeType, "boundary");
                    if (boundary == null) {
                        LOG.warn("Could not find boundary in multipart request with ContentType: {}, multipart data will not be available", mimeType);
                    }
                } else {
                    isMultipart = false;
                    boundary = null;
                }
                exchange.addRequestWrapper((factory, wrappedExchange) -> new RequestBodySaverStreamSourceConduit(
                        factory.create(),
                        isMultipart,
                        boundary,
                        () -> new RequestBodyPart(isMultipart ? null : mimeType),
                        requestParts -> wrappedExchange.putAttachment(
                                SAVED_REQUEST_BODY,
                                requestParts.stream()
                                        .filter(rp -> supportedMimeTypes.contains(rp.partMimeType))
                                        .collect(Collectors.toList()))));
            }
        }
        next.handleRequest(exchange);
    }

}
