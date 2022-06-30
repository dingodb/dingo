package io.dingodb.sdk.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tags a field in a class to be mapped to a Dingo column.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DingoColumn {
    /**
     * The name of the column to use. If not specified, the field name is used for the column name.
     */
    String name() default "";

    boolean useAccessors() default false;
}
