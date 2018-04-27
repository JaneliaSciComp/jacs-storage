package org.janelia.jacsstorage.interceptors;

import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

@Timed
@Interceptor
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
        try {
            m = invocationContext.getMethod();
            Parameter[] parameters = m.getParameters();
            Object[] parameterValues = invocationContext.getParameters();
            populateLogData(m, parameters, parameterValues, invocationResult, logData);
        } catch (Exception e) {
            LOG.warn("Error while trying to get method to time access", e);
        } finally {
            if (logData.isEmpty())
                LOG.info("Accessed method: {} - {} ms", m, (completedAccess - startAccess) / 1000000.);
            else
                LOG.info("Accessed method: {} with {}  - {} ms", m, logData, (completedAccess - startAccess) / 1000000.);
        }
        if (me != null) {
            throw me;
        } else {
            return invocationResult;
        }
    }

    private void populateLogData(Method m, Parameter[] parameters, Object[] parameterValues, Object methodResult, Map<String, String> logData) {
        boolean includeResult = false;
        IntStream argStream;
        TimedMethod timedMethodAnnotation = m.getAnnotation(TimedMethod.class);
        if (timedMethodAnnotation != null) {
            if (timedMethodAnnotation.argList().length > 0) {
                argStream = Arrays.stream(timedMethodAnnotation.argList());
            } else {
                argStream = IntStream.range(0, parameters.length);
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
