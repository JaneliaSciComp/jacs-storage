package org.janelia.jacsstorage.app;

import java.util.Set;

import jakarta.ws.rs.core.Application;

import com.fasterxml.jackson.jakarta.rs.json.JacksonXmlBindJsonProvider;
import com.google.common.collect.ImmutableSet;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.filter.CORSResponseFilter;
import org.janelia.jacsstorage.provider.ObjectMapperResolver;

public abstract class AbstractJAXApp extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return ImmutableSet.<Class<?>>builder()
                .add(OpenApiResource.class)
                .add(ObjectMapperResolver.class)
                .add(JacksonXmlBindJsonProvider.class)
                .add(AuthFilter.class)
                .add(CORSResponseFilter.class)
                .addAll(getAppClasses())
                .build();
    }

    protected abstract Set<Class<?>> getAppClasses();

}
