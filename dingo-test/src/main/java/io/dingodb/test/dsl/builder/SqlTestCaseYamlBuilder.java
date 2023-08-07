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

package io.dingodb.test.dsl.builder;

import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.test.utils.ResourceFileUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;

public class SqlTestCaseYamlBuilder extends SqlTestCaseBuilder {
    private static final Parser PARSER = Parser.YAML;

    private SqlTestCaseYamlBuilder(String name, Class<?> callerClass, String basePath) {
        super(name);
        InputStream is = ResourceFileUtils.getResourceFile(basePath + "/" + name, callerClass);
        try {
            context = PARSER.parse(is, SqlBuildingContext.class);
            context.setCallerClass(callerClass);
            context.setBasePath(basePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static @NonNull SqlTestCaseYamlBuilder of(@NonNull String path) {
        final Class<?> clazz = ResourceFileUtils.getCallerClass();
        String basePath;
        String fileName;
        int index = path.lastIndexOf('/');
        if (index < 0) {
            basePath = ".";
            fileName = path;
        } else {
            basePath = path.substring(0, index);
            fileName = path.substring(index + 1);
        }
        return new SqlTestCaseYamlBuilder(fileName, clazz, basePath);
    }
}
