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

package io.dingodb.calcite.utils;

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DingoEnumUtils {
    public static List<RelDataType> fieldRowTypes(
        final RelDataType inputRowType,
        final @Nullable List<? extends RexNode> extraInputs,
        final List<Integer> argList) {
        final List<RelDataTypeField> inputFields = inputRowType.getFieldList();
        return new AbstractList<RelDataType>() {
            @Override public RelDataType get(int index) {
                final int arg = argList.get(index);
                return arg < inputFields.size()
                    ? inputFields.get(arg).getType()
                    : requireNonNull(extraInputs, "extraInputs")
                    .get(arg - inputFields.size()).getType();
            }
            @Override public int size() {
                return argList.size();
            }
        };
    }

    public static Type javaClass(
        JavaTypeFactory typeFactory, RelDataType type) {
        final Type clazz = typeFactory.getJavaClass(type);
        return clazz instanceof Class ? clazz : Object[].class;
    }

    public static List<Type> fieldTypes(
        final JavaTypeFactory typeFactory,
        final List<? extends RelDataType> inputTypes) {
        return new AbstractList<Type>() {
            @Override public Type get(int index) {
                return javaClass(typeFactory, inputTypes.get(index));
            }
            @Override public int size() {
                return inputTypes.size();
            }
        };
    }

}
