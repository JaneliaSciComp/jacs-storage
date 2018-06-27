package org.janelia.jacsstorage.provider;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;

import javax.inject.Inject;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperResolver extends JacksonJaxbJsonProvider {

    @Inject
    public ObjectMapperResolver(ObjectMapperFactory mapperFactory) {
        setMapper(
                mapperFactory.newObjectMapper()
        );
    }

}
