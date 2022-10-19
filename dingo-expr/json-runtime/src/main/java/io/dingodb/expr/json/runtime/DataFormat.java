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

package io.dingodb.expr.json.runtime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@Getter
public enum DataFormat {
    APPLICATION_JSON("application/json"),
    APPLICATION_YAML("application/yaml"),
    TEXT_CSV("text/csv");

    @JsonValue
    private final String value;

    DataFormat(String value) {
        this.value = value;
    }

    /**
     * Get a DataFormat enum from a String. {@code "application/json"} -&gt; {@code APPLICATION_JSON} {@code
     * "application/yaml"} -&gt; {@code APPLICATION_YAML}
     *
     * @param str the string
     * @return the DataFormat
     */
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    public static @NonNull DataFormat fromString(String str) {
        for (DataFormat dataFormat : DataFormat.values()) {
            if (str.equals(dataFormat.getValue())) {
                return dataFormat;
            }
        }
        throw new IllegalArgumentException("Invalid string value \"" + str
            + "\" for enum type \"" + DataFormat.class.getSimpleName() + "\".");
    }

    /**
     * Get a DataFormat enum according a fileName. {@code "*.json"} -&gt; {@code APPLICATION_JSON} {@code "*.yaml"}
     * -&gt; {@code APPLICATION_YAML} {@code "*.yml"} -&gt; {@code APPLICATION_YAML}
     *
     * @param fileName the file name
     * @return the DataFormat
     */
    public static @NonNull DataFormat fromExtension(@NonNull String fileName) {
        if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
            return APPLICATION_YAML;
        } else if (fileName.endsWith(".json")) {
            return APPLICATION_JSON;
        } else if (fileName.endsWith(".csv")) {
            return TEXT_CSV;
        }
        throw new IllegalArgumentException("Invalid extension of file name \""
            + fileName + "\" for enum type \"" + DataFormat.class.getSimpleName() + "\".");
    }

    @Override
    public String toString() {
        return value;
    }
}
