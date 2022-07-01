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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GenerateTypeCodesProcessor extends AbstractProcessor {
    private static final String LOOKUP_METHOD = "nameOf";
    private static final String LOOKUP_PARA = "code";
    private static final String REV_LOOKUP_METHOD = "codeOf";
    private static final String REV_LOOKUP_PARA = "name";

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return ImmutableSet.<String>builder()
            .add(GenerateTypeCodes.class.getName())
            .build();
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations == null) {
            return true;
        }
        for (TypeElement annotation : annotations) {
            if (annotation.getQualifiedName().contentEquals(GenerateTypeCodes.class.getCanonicalName())) {
                try {
                    TypeCodeInfo typeCodeInfo = TypeCodeInfo.createFromEnv(roundEnv);
                    TypeSpec.Builder classBuilder = TypeSpec.classBuilder(typeCodeInfo.getClassName())
                        .addModifiers(Modifier.PUBLIC, Modifier.FINAL);
                    CodeBlock.Builder lookupMethodBuilder = CodeBlock.builder();
                    CodeBlock.Builder revLookupMethodBuilder = CodeBlock.builder();
                    lookupMethodBuilder.add("switch ($L) {\n", LOOKUP_PARA);
                    revLookupMethodBuilder.add("switch ($L) {\n", REV_LOOKUP_PARA);
                    for (Map.Entry<Integer, List<String>> entry : typeCodeInfo.getNameMap().entrySet()) {
                        List<String> names = entry.getValue();
                        assert !names.isEmpty();
                        String name = names.get(0);
                        int code = entry.getKey();
                        classBuilder.addField(FieldSpec.builder(TypeName.INT, name)
                            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                            // Must be a constant to be used in case statement.
                            .initializer("$L", code)
                            .build());
                        // The first name is the constant just defined.
                        lookupMethodBuilder.add("    case $L:\n        return $S;\n", name, name);
                        for (String n : names) {
                            revLookupMethodBuilder.add("    case $S:\n        return $L;\n", n, name);
                        }
                    }
                    lookupMethodBuilder.add("    default:\n        break;\n}\n");
                    lookupMethodBuilder.addStatement("throw new $T($S + $L + $S)",
                        IllegalArgumentException.class,
                        "Unrecognized type code \"", LOOKUP_PARA, "\"."
                    );
                    revLookupMethodBuilder.add("    default:\n        break;\n}\n");
                    revLookupMethodBuilder.addStatement("throw new $T($S + $L +$S)",
                        IllegalArgumentException.class,
                        "Unrecognized type name \"", REV_LOOKUP_PARA, "\"."
                    );
                    classBuilder
                        .addMethod(MethodSpec.constructorBuilder()
                            .addModifiers(Modifier.PRIVATE)
                            .build())
                        .addMethod(MethodSpec.methodBuilder(LOOKUP_METHOD)
                            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                            .addParameter(TypeName.INT, "code")
                            .returns(String.class)
                            .addCode(lookupMethodBuilder.build())
                            .build())
                        .addMethod(MethodSpec.methodBuilder(REV_LOOKUP_METHOD)
                            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                            .addParameter(TypeName.get(String.class), "name")
                            .returns(TypeName.INT)
                            .addCode(revLookupMethodBuilder.build())
                            .build());
                    ProcessorUtils.saveSourceFile(processingEnv, typeCodeInfo.getClassName().packageName(),
                        classBuilder.build());
                } catch (Exception e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage());
                }
            }
        }
        return true;
    }
}
