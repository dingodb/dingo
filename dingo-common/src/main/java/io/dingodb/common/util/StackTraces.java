/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.common.util;

import java.util.Scanner;
import java.util.StringJoiner;

/**
 * Display stack trace about current thread.
 * Default 0.
 * 0) StackTraces method.
 * 1) Current invoke method.
 */
public final class StackTraces {

    public static final int CURRENT_STACK = 1;

    private StackTraces() {
    }

    public static String methodName() {
        return methodName(CURRENT_STACK + 1);
    }

    public static String methodName(int stack) {
        return Thread.currentThread().getStackTrace()[CURRENT_STACK + stack].getMethodName();
    }

    public static int lineNumber() {
        return lineNumber(CURRENT_STACK + 1);
    }

    public static int lineNumber(int stack) {
        return Thread.currentThread().getStackTrace()[CURRENT_STACK + stack].getLineNumber();
    }

    public static Class<?> clazz() throws ClassNotFoundException {
        return clazz(CURRENT_STACK + 1);
    }

    public static Class<?> clazz(int stack) throws ClassNotFoundException {
        return Class.forName(className(stack + 1));
    }

    public static String className() {
        return className(CURRENT_STACK + 1);
    }

    public static String className(int stack) {
        return Thread.currentThread().getStackTrace()[CURRENT_STACK + stack].getClassName();
    }

    public static String fileName() {
        return fileName(CURRENT_STACK + 1);
    }

    public static String fileName(int stack) {
        return Thread.currentThread().getStackTrace()[CURRENT_STACK + stack].getFileName();
    }

    public static String packageName() {
        return packageName(CURRENT_STACK + 1);
    }

    public static String packageName(int stack) {
        String className = className(stack);
        return className.substring(0, className.lastIndexOf('.'));
    }

    public static String stack() {
        return stack(CURRENT_STACK + 1);
    }

    public static String stack(int stack) {
        return String.format("%s.%s:%s", className(stack + 1), methodName(stack + 1), lineNumber(stack + 1));
    }

    public static String stackTrace() {
        return stackTrace(CURRENT_STACK + 1, Integer.MAX_VALUE - CURRENT_STACK - 1);
    }

    public static String stackTrace(int stack, int deep) {
        return formatStackTrace(Thread.currentThread().getStackTrace(), stack, deep);
    }

    public static String formatStackTrace(StackTraceElement[] stackTrace, int stack, int deep) {
        StringJoiner joiner = new StringJoiner("\n    =======> ", "[\n    |||||||| ", "\n]");
        deep = (deep += stack) < 0 ? stackTrace.length - 1 : Math.min(deep, stackTrace.length - 1);
        for (int i = deep; i > stack; i--) {
            joiner.add(stackTrace[i].toString());
        }
        return joiner.toString();
    }

}
