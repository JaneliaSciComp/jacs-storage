package org.janelia.jacsstorage.app;

import java.util.Set;

import jakarta.ws.rs.core.Application;

import com.fasterxml.jackson.jaxrs.xml.JacksonJaxbXMLProvider;
import com.google.common.collect.ImmutableSet;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.filter.CORSResponseFilter;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;
import org.janelia.jacsstorage.rest.IOExceptionRequestHandler;
import org.janelia.jacsstorage.rest.IllegalAccessRequestHandler;
import org.janelia.jacsstorage.rest.IllegalStateRequestHandler;
import org.janelia.jacsstorage.rest.InvalidArgumentRequestHandler;
import org.janelia.jacsstorage.rest.InvalidJsonRequestHandler;
import org.janelia.jacsstorage.rest.JsonParseErrorRequestHandler;
import org.janelia.jacsstorage.rest.NotFoundRequestHandler;
import org.janelia.jacsstorage.rest.UncheckedIOExceptionRequestHandler;
import org.janelia.jacsstorage.rest.UnexpectedRuntimeExceptionRequestHandler;
import org.janelia.jacsstorage.rest.WebAppExceptionRequestHandler;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(OpenApiResource.class,
                        ObjectMapperResolver.class,
                        JacksonJaxbXMLProvider.class,
                        AuthFilter.class,
                        CORSResponseFilter.class,
                        InvalidArgumentRequestHandler.class,
                        IllegalAccessRequestHandler.class,
                        IllegalStateRequestHandler.class,
                        InvalidJsonRequestHandler.class,
                        JsonParseErrorRequestHandler.class,
                        IOExceptionRequestHandler.class,
                        NotFoundRequestHandler.class,
                        UncheckedIOExceptionRequestHandler.class,
                        WebAppExceptionRequestHandler.class,
                        UnexpectedRuntimeExceptionRequestHandler.class
                )
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
