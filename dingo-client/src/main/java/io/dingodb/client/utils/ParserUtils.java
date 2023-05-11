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

package io.dingodb.client.utils;

public class ParserUtils {
    private static final ParserUtils instance = new ParserUtils();

    public static ParserUtils getInstance() {
        return instance;
    }

    private ParserUtils() {
    }

    public String get(String value) {
        return parseString(value);
    }

    private String parseString(String value) {
        if (value == null || value.length() <= 3) {
            return value;
        }
        if ((value.startsWith("${") || value.startsWith("#{")) && value.endsWith("}")) {
            // Strip off the identifying tokens and split into value and default
            String[] values = value.substring(2, value.length() - 1).split(":");

            String translatedValue;
            if (value.startsWith("${")) {
                translatedValue = System.getProperty(values[0]);
            } else {
                translatedValue = System.getenv(values[0]);
            }

            if (translatedValue != null) {
                return translatedValue;
            }
            if (values.length > 1) {
                // A default was provided, use it.
                return values[1];
            }
            // No environment/property variable was found, no default, return null
            return null;
        }
        return value;
    }
}
