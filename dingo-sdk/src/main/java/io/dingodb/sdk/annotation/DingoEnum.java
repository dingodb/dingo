package io.dingodb.sdk.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tags an enum field of a class to set a value field to store its value in the Dingo database instead of the enum constant name.
 * default behaviour (without using this annotation or without specifying the value field's name) is to
 * store the entire constant name in the Dingo database.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DingoEnum {
    String enumField() default "";
}
