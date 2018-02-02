package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.function.BiConsumer;

@Logged
@Interceptor
public class LoggerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerInterceptor.class);
    private final StorageEventLogger storageEventLogger;

    @Inject
    public LoggerInterceptor(StorageEventLogger storageEventLogger) {
        this.storageEventLogger = storageEventLogger;
    }

    @AroundInvoke
    public Object logStorageEvent(InvocationContext invocationContext) throws Exception {
        Exception invocationException = null;
        try {
            return invocationContext.proceed();
        } catch (Exception e) {
            invocationException = e;
            throw e;
        } finally {
            logEvent(invocationContext, invocationException);
        }
    }

    private void logEvent(InvocationContext invocationContext, Throwable invocationException) {
        try {
            String eventName = null;
            String eventDescription = null;
            Method m = invocationContext.getMethod();
            LogStorageEvent logAnnotation = m.getAnnotation(LogStorageEvent.class);
            Set<Integer> argIndexSet;
            if (logAnnotation != null) {
                eventName = logAnnotation.eventName();
                eventDescription = logAnnotation.description();
                ImmutableSet.Builder<Integer> argIndexBuilder = ImmutableSet.builder();
                for (int argIndex : logAnnotation.argList()) {
                    argIndexBuilder.add(argIndex);
                }
                argIndexSet = argIndexBuilder.build();
            } else {
                argIndexSet = ImmutableSet.of();
            }
            if (StringUtils.isBlank(eventName)) {
                eventName = m.getName();
            }
            ImmutableList.Builder<Object> eventDataBuilder = ImmutableList.builder();
            int argIndex = 0;
            for (Object methodParam : invocationContext.getParameters()) {
                if (argIndexSet.contains(argIndex)) {
                    if (methodParam == null) {
                        eventDataBuilder.add("<null>");
                    } else {
                        eventDataBuilder.add(methodParam);
                    }
                }
                argIndex++;
            }
            storageEventLogger.logStorageEvent(
                    eventName,
                    eventDescription,
                    eventDataBuilder.build(),
                    invocationException == null ? "SUCCESS" : "FAILURE: " + invocationException.getMessage()
            );
        } catch (Exception e) {
            LOG.warn("Error while trying to log storage event", e);
        }
    }
}
