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

package io.dingodb.expr.annotations;

import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeSpec;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import javax.annotation.processing.ProcessingEnvironment;

public final class ProcessorUtils {
    static final String INSTANCE_VAR_NAME = "INSTANCE";

    private ProcessorUtils() {
    }

    /**
     * Save a source file.
     *
     * @param processingEnv the ProcessingEnvironment
     * @param packageName   the package name
     * @param typeSpec      the TypeSpec of the class/interface
     */
    public static void saveSourceFile(
        @NonNull ProcessingEnvironment processingEnv,
        String packageName,
        TypeSpec typeSpec
    ) throws IOException {
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
        javaFile.writeTo(processingEnv.getFiler());
    }
}
