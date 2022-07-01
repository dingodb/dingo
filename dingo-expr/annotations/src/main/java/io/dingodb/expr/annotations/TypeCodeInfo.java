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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;

@RequiredArgsConstructor
public class TypeCodeInfo {
    private static final String TYPE_CODE_CLASS_NAME = "TypeCode";

    @Getter
    private final ClassName className;
    @Getter
    private final Map<Integer, List<String>> nameMap;

    /**
     * Get the type code of a type.
     *
     * @param type the name of the type
     * @return the type code
     */
    public static int typeCode(@Nonnull String type) {
        String name = type.replaceAll("<.*>", "");
        return name.hashCode();
    }

    /**
     * Get the type code of a TypeName.
     *
     * @param type the TypeName
     * @return the type code
     */
    public static int typeCode(@Nonnull TypeName type) {
        return typeCode(type.toString());
    }

    @Nonnull
    public static TypeCodeInfo createFromEnv(@Nonnull RoundEnvironment roundEnv) {
        Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(GenerateTypeCodes.class);
        if (elements.size() != 1) {
            throw new IllegalStateException(
                "There must be one and only one element annotated with \"GenerateTypeCodes\""
            );
        }
        Element pkg = null;
        for (Element element : elements) {
            pkg = element;
            break;
        }
        assert pkg != null;
        if (pkg.getKind() != ElementKind.PACKAGE) {
            throw new IllegalStateException(
                "Only packages can be annotated with \"GenerateTypeCodes\"."
            );
        }
        ClassName className = ClassName.get(pkg.asType().toString(), TYPE_CODE_CLASS_NAME);
        Map<Integer, List<String>> map = new HashMap<>();
        GenerateTypeCodes generateTypeCodes = pkg.getAnnotation(GenerateTypeCodes.class);
        for (GenerateTypeCodes.TypeCode typeCode : generateTypeCodes.value()) {
            String name = typeCode.name();
            String[] aliases = typeCode.aliases();
            List<String> names = new ArrayList<>(aliases.length + 1);
            names.add(name);
            names.addAll(Arrays.asList(aliases));
            int code = typeCode(typeCode.type());
            map.put(code, names);
        }
        return new TypeCodeInfo(className, map);
    }

    @Nonnull
    public CodeBlock evaluatorKeyOf(
        TypeElement evaluatorKey,
        @Nonnull List<TypeName> paraTypeNames
    ) {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("$T.of(", evaluatorKey);
        boolean addComma = false;
        for (TypeName paraTypeName : paraTypeNames) {
            if (addComma) {
                builder.add(", ");
            }
            builder.add(typeOf(paraTypeName));
            addComma = true;
        }
        builder.add(")");
        return builder.build();
    }

    @Nonnull
    public CodeBlock typeOf(TypeName typeName) {
        CodeBlock.Builder builder = CodeBlock.builder();
        List<String> names = nameMap.get(typeCode(typeName));
        if (names == null || names.isEmpty()) {
            throw new IllegalArgumentException("Type name \"" + typeName + "\" is not supported.");
        }
        builder.add("$T.$L", className, names.get(0));
        return builder.build();
    }
}
