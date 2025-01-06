package org.janelia.jacsstorage.interceptors;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import jakarta.enterprise.context.Dependent;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timed
@Interceptor
@Dependent
public class TimedInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(TimedInterceptor.class);

    @AroundInvoke
    public Object timeAccess(InvocationContext invocationContext) throws Exception {
        Exception me = null;
        long startAccess = System.nanoTime();
        long completedAccess;
        Object invocationResult = null;
        try {
            invocationResult = invocationContext.proceed();
        } catch (Exception e) {
            me = e;
        } finally {
            completedAccess = System.nanoTime();
        }
        Method m = null;
        Map<String, String> logData = new LinkedHashMap<>();
        String logLevel = "debug";
        try {
            m = invocationContext.getMethod();
            TimedMethod timedMethodAnnotation = m.getAnnotation(TimedMethod.class);
            if (timedMethodAnnotation != null) {
                logLevel = timedMethodAnnotation.logLevel();
            }
            Parameter[] parameters = m.getParameters();
            Object[] parameterValues = invocationContext.getParameters();
            populateLogData(m, parameters, parameterValues, invocationResult, timedMethodAnnotation, logData);
        } catch (Exception e) {
            LOG.warn("Error while trying to get method to time access", e);
        } finally {
            logTimingInfo(m, logData, (completedAccess - startAccess) / 1000000., logLevel);
        }
        if (me != null) {
            throw me;
        } else {
            return invocationResult;
        }
    }

    private void logTimingInfo(Method method, Map<String, String> logData, double accessTimeInMillis, String logLevel) {
        Class<?> mDeclaringClass = method.getDeclaringClass();
        Logger logger = LoggerFactory.getLogger(mDeclaringClass);
        if (logLevel.equalsIgnoreCase("info")) {
            logger.info("Accessed method: {}.{} with {}  - {} ms",
                    mDeclaringClass.getSimpleName(), method.getName(), logData, accessTimeInMillis);
        } else if (logLevel.equalsIgnoreCase("debug")) {
            logger.debug("Accessed method: {}.{} with {}  - {} ms",
                    mDeclaringClass.getSimpleName(), method.getName(), logData, accessTimeInMillis);
        } else {
            logger.trace("Accessed method: {}.{} with {}  - {} ms",
                    mDeclaringClass.getSimpleName(), method.getName(), logData, accessTimeInMillis);
        }
    }

    private void populateLogData(Method m, Parameter[] parameters,
                                 Object[] parameterValues,
                                 Object methodResult,
                                 TimedMethod timedMethodAnnotation,
                                 Map<String, String> logData) {
        boolean includeResult = false;
        IntStream argStream;
        if (timedMethodAnnotation != null) {
            if (!timedMethodAnnotation.logArgs()) {
                argStream = IntStream.empty();
            } else {
                if (timedMethodAnnotation.argList().length > 0) {
                    argStream = Arrays.stream(timedMethodAnnotation.argList());
                } else {
                    argStream = IntStream.range(0, parameters.length);
                }
            }
            if (timedMethodAnnotation.logResult()) {
                includeResult = true;
            }
        } else {
            argStream = IntStream.range(0, parameters.length);
        }
        argStream.forEach(argIndex -> {
            logData.put(parameters[argIndex].getName(), Objects.toString(parameterValues[argIndex]));
        });
        if (includeResult) {
            logData.put(m.getName() + "_result", Objects.toString(methodResult));
        }
    }
}
