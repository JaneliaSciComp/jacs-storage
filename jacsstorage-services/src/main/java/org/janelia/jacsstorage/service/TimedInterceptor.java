package org.janelia.jacsstorage.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

@Timed
@Interceptor
public class TimedInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(TimedInterceptor.class);

    @AroundInvoke
    public Object timeAccess(InvocationContext invocationContext) throws Exception {
        long startAccess = System.nanoTime();
        Method m = null;
        try {
            m = invocationContext.getMethod();
        } catch (Exception e) {
            LOG.warn("Error while trying to get method to time access", e);
        }
        try {
            return invocationContext.proceed();
        } finally {
            long completedAccess = System.nanoTime();
            LOG.info("Accessed method: {} - {} ms", m, (completedAccess - startAccess) / 1000000.);
        }
    }
}
