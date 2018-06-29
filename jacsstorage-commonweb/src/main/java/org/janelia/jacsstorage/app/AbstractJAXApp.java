package org.janelia.jacsstorage.app;

import com.fasterxml.jackson.jaxrs.xml.JacksonJaxbXMLProvider;
import com.google.common.collect.ImmutableSet;
import io.swagger.jaxrs.listing.ApiListingResource;
import org.janelia.jacsstorage.filter.CORSResponseFilter;
import org.janelia.jacsstorage.filter.JWTAuthFilter;
import org.janelia.jacsstorage.io.DataAlreadyExistException;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;
import org.janelia.jacsstorage.rest.IOErrorRequestHandler;
import org.janelia.jacsstorage.rest.IllegalAccessRequestHandler;
import org.janelia.jacsstorage.rest.IllegalStateRequestHandler;
import org.janelia.jacsstorage.rest.InvalidArgumentRequestHandler;
import org.janelia.jacsstorage.rest.InvalidJsonRequestHandler;
import org.janelia.jacsstorage.rest.JsonParseErrorRequestHandler;

import javax.ws.rs.core.Application;
import java.util.Set;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(ApiListingResource.class,
                        ObjectMapperResolver.class,
                        JacksonJaxbXMLProvider.class,
                        JWTAuthFilter.class,
                        CORSResponseFilter.class,
                        DataAlreadyExistException.class,
                        InvalidArgumentRequestHandler.class,
                        IllegalAccessRequestHandler.class,
                        IllegalStateRequestHandler.class,
                        InvalidJsonRequestHandler.class,
                        JsonParseErrorRequestHandler.class,
                        IOErrorRequestHandler.class)
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
