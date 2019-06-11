package org.janelia.jacsstorage.app;

import com.fasterxml.jackson.jaxrs.xml.JacksonJaxbXMLProvider;
import com.google.common.collect.ImmutableSet;
import io.swagger.jaxrs.listing.ApiListingResource;
import org.janelia.jacsstorage.filter.CORSResponseFilter;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;
import org.janelia.jacsstorage.rest.*;

import javax.ws.rs.core.Application;
import java.util.Set;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(ApiListingResource.class,
                        ObjectMapperResolver.class,
                        JacksonJaxbXMLProvider.class,
                        AuthFilter.class,
                        CORSResponseFilter.class,
                        DataAlreadyExistRequestHandler.class,
                        InvalidArgumentRequestHandler.class,
                        IllegalAccessRequestHandler.class,
                        IllegalStateRequestHandler.class,
                        InvalidJsonRequestHandler.class,
                        JsonParseErrorRequestHandler.class,
                        IOExceptionRequestHandler.class,
                        NotFoundRequestHandler.class,
                        UncheckedIOExceptionRequestHandler.class,
                        UnexpectedRuntimeExceptionRequestHandler.class
                )
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
