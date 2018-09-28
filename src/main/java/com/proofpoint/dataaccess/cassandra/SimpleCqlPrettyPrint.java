package com.proofpoint.dataaccess.cassandra;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SimpleCqlPrettyPrint
{
    public String value() default "";

    public boolean sensitive() default false;

    public boolean ignore() default false;

    public boolean custom() default false;

    public boolean ignore_null() default true;
}
