package org.janelia.jacsstorage.app;

import java.util.Set;

import jakarta.ws.rs.core.Application;

import com.fasterxml.jackson.jakarta.rs.json.JacksonXmlBindJsonProvider;
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
import org.janelia.jacsstorage.rest.NoContentFoundRequestHandler;
import org.janelia.jacsstorage.rest.NotFoundRequestHandler;
import org.janelia.jacsstorage.rest.UncheckedIOExceptionRequestHandler;
import org.janelia.jacsstorage.rest.UnexpectedRuntimeExceptionRequestHandler;
import org.janelia.jacsstorage.rest.WebAppExceptionRequestHandler;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(OpenApiResource.class)
                .add(ObjectMapperResolver.class)
                .add(JacksonXmlBindJsonProvider.class)
                .add(AuthFilter.class)
                .add(CORSResponseFilter.class)
                .add(InvalidArgumentRequestHandler.class)
                .add(IllegalAccessRequestHandler.class)
                .add(IllegalStateRequestHandler.class)
                .add(InvalidJsonRequestHandler.class)
                .add(JsonParseErrorRequestHandler.class)
                .add(IOExceptionRequestHandler.class)
                .add(NotFoundRequestHandler.class)
                .add(NoContentFoundRequestHandler.class)
                .add(UncheckedIOExceptionRequestHandler.class)
                .add(WebAppExceptionRequestHandler.class)
                .add(UnexpectedRuntimeExceptionRequestHandler.class)
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
