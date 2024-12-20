package org.janelia.jacsstorage.interceptors.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;

@Inherited
@Timed
@InterceptorBinding
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface TimedMethod {
    /**
     * @return the list of arguments to record together with the timing information.
     */
    @Nonbinding int[] argList() default {};

    @Nonbinding boolean logArgs() default true;

    @Nonbinding boolean logResult() default false;

    @Nonbinding String logLevel() default "debug";
}
