package org.janelia.jacsstorage.filter;

import java.io.IOException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

@Provider
public class CORSResponseFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext resp) throws IOException {
        resp.getHeaders().add("Access-Control-Allow-Origin","*");
        resp.getHeaders().add("Access-Control-Allow-Methods","GET,POST,PUT,DELETE");
        resp.getHeaders().add("Access-Control-Allow-Headers","Origin, X-Requested-With, Content-Type, Accept, Authorization, Application-Id, Content-Disposition");
    }

}
