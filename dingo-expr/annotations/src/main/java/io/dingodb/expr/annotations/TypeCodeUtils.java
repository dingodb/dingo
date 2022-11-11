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

import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import io.dingodb.expr.core.TypeCode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import javax.lang.model.element.TypeElement;

public final class TypeCodeUtils {
    private TypeCodeUtils() {
    }

    /**
     * Get the type code of a type.
     *
     * @param type the name of the type
     * @return the type code
     */
    public static int typeCode(@NonNull String type) {
        // Remove generic parameters
        String name = type.replaceAll("<.*>", "");
        // Special case
        if (type.equals("byte[]")) {
            return TypeCode.BINARY;
        } else if (type.endsWith("[]")) {
            return TypeCode.ARRAY;
        }
        try {
            Class<?> clazz = Class.forName(name);
            return TypeCode.codeOf(clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unrecognized type \"" + type + "\".");
        }
    }

    /**
     * Get the type code of a TypeName.
     *
     * @param type the TypeName
     * @return the type code
     */
    public static int typeCode(@NonNull TypeName type) {
        return typeCode(type.toString());
    }

    public static @NonNull CodeBlock evaluatorKeyOf(
        TypeElement evaluatorKey,
        @NonNull List<TypeName> paraTypeNames
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

    public static @NonNull CodeBlock typeOf(TypeName typeName) {
        CodeBlock.Builder builder = CodeBlock.builder();
        String name = TypeCode.nameOf(typeCode(typeName));
        builder.add("$T.$L", TypeCode.class, name);
        return builder.build();
    }
}
