package org.janelia.jacsstorage.cdi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class CoreCdiProducer {
    @Produces
    public Logger createLogger(final InjectionPoint ip){
        return LoggerFactory.getLogger(ip.getMember().getDeclaringClass());
    }

    @Produces
    public ExecutorService createStorageAgentExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }
}
