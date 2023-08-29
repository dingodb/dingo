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

package io.dingodb.test.utils;

import org.apache.commons.io.IOUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class ResourceFileUtils {
    private ResourceFileUtils() {
    }

    public static @NonNull InputStream getResourceFile(String fileName, @NonNull Class<?> clazz) {
        InputStream is = clazz.getResourceAsStream(fileName);
        if (is != null) {
            return is;
        }
        throw new RuntimeException("Cannot access file \"" + fileName
            + "\" in resources of class \"" + clazz.getCanonicalName() + "\".");
    }

    public static @NonNull Class<?> getCallerClass() {
        try {
            throw new Exception();
        } catch (Exception exception) {
            // The class call the method which call this method is wanted.
            String className = exception.getStackTrace()[2].getClassName();
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Something unbelievable happened.");
            }
        }
    }

    public static @NonNull String readString(InputStream sqlFile) {
        try {
            return IOUtils.toString(sqlFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
