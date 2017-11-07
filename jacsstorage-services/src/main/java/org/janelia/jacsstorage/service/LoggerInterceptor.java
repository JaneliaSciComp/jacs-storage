package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;

@Logged
@Interceptor
public class LoggerInterceptor {

    private final StorageEventLogger storageEventLogger;

    @Inject
    public LoggerInterceptor(StorageEventLogger storageEventLogger) {
        this.storageEventLogger = storageEventLogger;
    }

    @AroundInvoke
    public Object logStorageEvent(InvocationContext invocationContext) throws Exception {
        String eventName = null;
        String eventDescription = null;
        Method m = invocationContext.getMethod();
        LogStorageEvent logAnnotation = m.getAnnotation(LogStorageEvent.class);
        if (logAnnotation != null) {
            eventName = logAnnotation.eventName();
            eventDescription = logAnnotation.description();
        }
        if (StringUtils.isBlank(eventName)) {
            eventName = m.getName();
        }
        ImmutableList.Builder eventDataBuilder = ImmutableList.builder();
        for (Object methodParam : invocationContext.getParameters()) {
            if (methodParam == null) {
                eventDataBuilder.add("<null>");
            } else {
                eventDataBuilder.add(methodParam);
            }
        }
        storageEventLogger.logStorageEvent(
                eventName,
                eventDescription,
                eventDataBuilder.build()
        );
        return invocationContext.proceed();
    }
}
